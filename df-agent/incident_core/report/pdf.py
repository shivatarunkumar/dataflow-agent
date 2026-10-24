"""Minimal Markdown → PDF (headings, paragraphs, bullets, tables, code blocks) with reportlab."""

import re
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle


# The built-in PDF fonts (Helvetica/Courier, WinAnsi) can't draw these; map them to ASCII.
_ASCII = str.maketrans({"→": "->", "←": "<-", "⇄": "<->", "▶": ">", "■": "#", "✖": "x", "✓": "ok", "…": "...",
                        "≤": "<=", "≥": ">=", "─": "-", "│": "|", "┬": "+", "┴": "+", "├": "+", "└": "+"})  # fmt: skip


def _ascii(text: str) -> str:
    return text.translate(_ASCII)


def _inline(text: str) -> str:
    text = escape(_ascii(text))
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"`([^`]+)`", r'<font face="Courier">\1</font>', text)
    return text


def markdown_to_pdf(markdown: str, path: str, title: str = "Incident report") -> None:
    styles = getSampleStyleSheet()
    body, small = styles["BodyText"], styles["BodyText"].clone("small", fontSize=8, leading=10)
    code = styles["Code"].clone("code_small", fontSize=7, leading=8.5)
    story, lines, i = [], markdown.splitlines(), 0

    while i < len(lines):
        line = lines[i]
        if line.startswith("```"):
            block, i = [], i + 1
            while i < len(lines) and not lines[i].startswith("```"):
                block.append(lines[i])
                i += 1
            story.append(Preformatted(_ascii("\n".join(block))[:20000], code))
        elif line.startswith("|"):
            rows = []
            while i < len(lines) and lines[i].startswith("|"):
                cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                if not all(re.fullmatch(r":?-{2,}:?", c) for c in cells if c):
                    rows.append([Paragraph(_inline(c), small) for c in cells])
                i += 1
            i -= 1
            if rows:
                width = max(len(r) for r in rows)
                rows = [r + [""] * (width - len(r)) for r in rows]
                table = Table(rows, colWidths=[(A4[0] - 30 * mm) / width] * width, repeatRows=1)
                table.setStyle(TableStyle([
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eeeeee")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]))  # fmt: skip
                story.append(table)
        elif m := re.match(r"^(#{1,3})\s+(.*)", line):
            story.append(Paragraph(_inline(m.group(2)), styles[f"Heading{len(m.group(1))}"]))
        elif m := re.match(r"^\s*(?:[-*]|\d+\.)\s+(.*)", line):
            indent = (len(line) - len(line.lstrip())) * 2 + 10
            story.append(Paragraph(_inline(m.group(1)), body.clone("bullet", leftIndent=indent, bulletIndent=indent - 8), bulletText="•"))
        elif line.strip():
            story.append(Paragraph(_inline(line), body))
        else:
            story.append(Spacer(1, 4))
        i += 1

    SimpleDocTemplate(path, pagesize=A4, title=title, leftMargin=15 * mm, rightMargin=15 * mm,
                      topMargin=15 * mm, bottomMargin=15 * mm).build(story)  # fmt: skip
