"""Sizes page images for Confluence's standard 760 px column, using each PNG's real dimensions.
Without the original height (or wider than the column) Confluence crops the picture. Safe to run twice.

Usage: python3 docs/pages/confluence_images.py <page folder> [...]
"""

import re
import struct
import sys
from pathlib import Path

COLUMN_WIDTH = 760
IMAGE = re.compile(r'<ac:image\b[^>]*?ac:alt="([^"]*)"[^>]*>(<ri:attachment ri:filename="([^"]+)"\s*/>)')


def png_size(path: Path) -> tuple:
    with path.open("rb") as f:
        return struct.unpack(">II", f.read(24)[16:24])


for folder in map(Path, sys.argv[1:]):
    page = folder / "confluence" / "page.xhtml"

    def sized(match: re.Match) -> str:
        width, height = png_size(folder / "confluence" / "attachments" / match[3])
        return (f'<ac:image ac:align="center" ac:layout="center" ac:alt="{match[1]}" ac:original-width="{width}" '
                f'ac:original-height="{height}" ac:width="{min(width, COLUMN_WIDTH)}">{match[2]}')

    page.write_text(IMAGE.sub(sized, page.read_text(encoding="utf-8")), encoding="utf-8")
