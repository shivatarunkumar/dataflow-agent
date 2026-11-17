"""Makes in-page links work on Confluence: `<a href="#slug">` has no target there, so add anchor macros to the
headings and turn the links into anchor links. Safe to run twice.

Usage: python3 docs/pages/confluence_anchors.py <page.xhtml> [...]
"""

import html
import re
import sys

HEADING = re.compile(r"<h([23])>([^<]*)</h\1>")
LINK = re.compile(r'<a href="#([^"]+)">([^<]*)</a>')


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9 -]", "", html.unescape(text).lower()).strip().replace(" ", "-")


def anchor_link(match: re.Match) -> str:
    return (f'<ac:link ac:anchor="{match[1]}"><ac:plain-text-link-body><![CDATA[{html.unescape(match[2])}]]>'
            "</ac:plain-text-link-body></ac:link>")


def anchored_heading(match: re.Match) -> str:
    macro = f'<ac:structured-macro ac:name="anchor"><ac:parameter ac:name="">{slug(match[2])}</ac:parameter></ac:structured-macro>'
    return f"<h{match[1]}>{macro}{match[2]}</h{match[1]}>"


for path in sys.argv[1:]:
    with open(path, encoding="utf-8") as f:
        page = f.read()
    with open(path, "w", encoding="utf-8") as f:
        f.write(HEADING.sub(anchored_heading, LINK.sub(anchor_link, page)))
