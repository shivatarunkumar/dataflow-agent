# Diagrams

Each diagram has these files with the same name:

| File | What it is |
|---|---|
| `<name>.json` | The diagram's source. Edit this (or ask the agent to) and re-run `/autodocs` to regenerate the others. Keep it in git. |
| `<name>.html` | Interactive diagram. Open in a browser: click components, trace paths, export PNG/SVG. |
| `<name>.drawio` | Editable draw.io file for Confluence or diagrams.net. |
| `<name>.png` | Static image for READMEs and pages that cannot embed the others. |

Check results from generation are kept in `../.checks/`, which git ignores.
