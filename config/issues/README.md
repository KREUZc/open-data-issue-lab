# Issue YAML Contract

Issue definitions live at:

```text
config/issues/issue-{issue-slug}.yaml
```

The MVP parser intentionally supports a small YAML subset so the project has no runtime dependency. The file should use:

- two-space indentation
- `key: value` mappings
- lists using `- key: value`
- quoted strings when values contain `:` or `#`

## Top-Level Fields

- `slug`: stable issue slug used in data output paths.
- `title`: public title.
- `question`: the human question this issue answers.
- `cadence`: expected pipeline cadence.
- `status_policy`: how the website behaves when the pipeline fails.
- `data_window`: bounded windows used by this issue.
- `sources`: source files or APIs.
- `metrics`: normalized metrics to produce.
- `charts`: chart definitions consumed by the frontend.
- `limits`: known limits that must appear near the data.

## Source Fields

- `id`: source id inside the issue.
- `dataset_id`: official data.gov.tw dataset id.
- `name`: public source name.
- `url`: file/API endpoint.
- `format`: `json` or `csv`.
- `encoding`: source encoding.
- `max_download_mb`: download guardrail.
- `parser`: parser function name in `scripts/fetch_issue_data.py`.
- `role`: how this source contributes to the issue.

## Output

The energy MVP writes:

```text
public/data/issues/energy.json
public/data/pipeline/latest-run.json
```

If a run fails but previous output exists, the script keeps the last successful data and marks it as stale.
