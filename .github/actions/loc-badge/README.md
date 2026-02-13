# loc-badge action

Generate a static SVG LOC badge using built-in JavaScript logic, optional commit/push, and step summary output.

## Features

- Generate LOC SVG badge at a configurable path
- Auto commit and push badge updates when content changes
- Support dry-run mode (compute and summarize without commit/push)

## Inputs

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `badge_path` | No | `.github/badges/loc.svg` | Output path for the generated SVG badge. |
| `scope` | No | `code` | LOC scope: `code`, `code_comments`, `all`. |
| `label` | No | `lines of code` | Badge label text. |
| `exclude` | No | `""` | Optional comma-separated extra excludes; `badge_path` is always excluded automatically. |
| `tokei_version` | No | `latest` | Tokei version for `cargo install` (e.g. `12.1.2`). |
| `dry_run` | No | `false` | If `true`, only generate badge and summary without commit/push. |
| `commit_message` | No | `ci(badge): refresh loc badge` | Commit message used when badge changes and `dry_run=false`. |
| `commit_user_name` | No | `github-actions[bot]` | Git author name for auto commit. |
| `commit_user_email` | No | `41898282+github-actions[bot]@users.noreply.github.com` | Git author email for auto commit. |
| `github_token` | No | `${{ github.token }}` | Token used for authenticated push. |
| `target_branch` | No | auto | Branch to push to; defaults to `GITHUB_HEAD_REF` or `GITHUB_REF_NAME`. |

## Outputs

| Name | Description |
| --- | --- |
| `changed` | Whether `badge_path` changed after generation. |
| `committed` | Whether a commit was created. |
| `pushed` | Whether the commit was pushed. |
| `value` | Computed LOC value (raw integer). |

## Example (auto commit/push)

```yaml
- uses: actions/checkout@v4

- uses: DCjanus/rdbinsight/.github/actions/loc-badge@master
```

## Example (dry run)

```yaml
- uses: actions/checkout@v4

- id: loc_badge
  uses: DCjanus/rdbinsight/.github/actions/loc-badge@master
  with:
    dry_run: true

- run: echo "LOC value = ${{ steps.loc_badge.outputs.value }}"
```
