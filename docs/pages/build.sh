#!/usr/bin/env bash
# Builds the Confluence pages in docs/pages/ (Markdown -> storage format + only the PNGs each page shows).
# Run from the repo root: bash docs/pages/build.sh
set -euo pipefail
AD="node ${AUTODOCS_ENGINE:-$HOME/.claude/skills/autodocs}/bin/autodocs.mjs"
build() {  # build <folder> <source.md> [png...]
  local dir=docs/pages/$1 src=$2; shift 2
  rm -rf "$dir/confluence"; mkdir -p "$dir/confluence/attachments"
  $AD confluence "$dir/$src" --out "$dir/confluence/page.xhtml" >/dev/null
  for png in "$@"; do cp "docs/autodocs/diagrams/$png" "$dir/confluence/attachments/"; done
  python3 docs/pages/confluence_anchors.py "$dir/confluence/page.xhtml"
  python3 docs/pages/confluence_images.py "$dir"
}
build main page.md architecture.png
build runbook runbook.md ingest-file.png job-status.png beam-pipeline.png agent-incident.png
build playbook playbook.md
