# Importing "dataflow-agent runbook" into Confluence

This folder is self-contained. Create one Confluence page for the runbook, then
attach the files under `attachments/` to that page.

## 1. Create the page

Pick one of these:

- **Storage format (keeps code blocks, panels and the table of contents).**
  Use the REST API or a source editor app with `page.xhtml`:

  ```bash
  curl -u "$CONFLUENCE_USER:$CONFLUENCE_API_TOKEN" -H 'Content-Type: application/json' \
    -X POST "$CONFLUENCE_BASE_URL/wiki/rest/api/content" \
    -d "$(jq -n --arg t 'dataflow-agent runbook' --arg s "$CONFLUENCE_SPACE" --rawfile b page.xhtml \
      '{type:"page",title:$t,space:{key:$s},body:{storage:{value:$b,representation:"storage"}}}')"
  ```

- **Markdown.** In the Confluence editor, choose *Insert → Markup → Markdown* and
  paste `page.md`.

## 2. Attach the files

Upload every file in `attachments/` to the page (*••• → Attachments → Upload*),
or with the REST API:

```bash
for f in attachments/*; do
  curl -u "$CONFLUENCE_USER:$CONFLUENCE_API_TOKEN" -H 'X-Atlassian-Token: no-check' \
    -X POST "$CONFLUENCE_BASE_URL/wiki/rest/api/content/$PAGE_ID/child/attachment" -F "file=@$f"
done
```

## 3. Place the draw.io diagrams

Each blue "Diagram" panel on the page names a `.drawio` file. Edit the page,
put the cursor on the panel, choose *Insert → draw.io Diagram → Import* (or
*File → Import from → Device* inside the draw.io editor), and select the file
from `attachments/`. The diagram stays editable in Confluence. This needs the
"draw.io Diagrams for Confluence" app on your site.

Interactive `.html` diagrams are attached too; viewers download and open them
in a browser for focus, path tracing and image export.

## Files

- `attachments/agent-incident.drawio`
- `attachments/agent-incident.html`
- `attachments/architecture.drawio`
- `attachments/architecture.html`
- `attachments/beam-pipeline.drawio`
- `attachments/beam-pipeline.html`
- `attachments/ingest-file.drawio`
- `attachments/ingest-file.html`
- `attachments/job-status.drawio`
- `attachments/job-status.html`
