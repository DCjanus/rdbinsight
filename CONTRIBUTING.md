## Contributing Guide (English)

English | [中文](CONTRIBUTING.zh_CN.md)

Thank you for your interest in contributing to RDBInsight! Please read and follow this guide before submitting code to keep the project consistent and high-quality.

### Quick Start

- Install and use `just` at the repository root (a `justfile` is provided).
- Before committing, run:

```bash
just before_commit
```

This command formats code, runs static checks, and executes tests to ensure your changes meet project requirements.

### Code Style & Quality

- Use `rustfmt` (nightly channel) for formatting.
- Keep dependency declarations sorted (`cargo sort`).
- Treat Clippy warnings as errors (`-D warnings`) and make sure they are cleared before committing.
- Prefer clear, readable implementations and avoid unnecessary complexity; add comments only for non-trivial business logic.
- Avoid deep nesting, which is a common pitfall for Rust beginners.
- Before writing new integration tests, first review the existing cases in the `test/` directory and prefer reusing the existing test infrastructure where possible.
- When updating documentation, ensure `CONTRIBUTING.md` and `CONTRIBUTING.zh_CN.md` remain content-aligned; likewise keep `README.md` and `README.zh_CN.md` in sync.

### Conventional Commits

- Commit messages should be concise English and follow the Conventional Commits specification.
- Message format: `<type>(<scope>): <subject>`
  - `<type>`: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`
  - `<scope>`: optional, indicates the affected area (e.g., a module name)
  - `<subject>`: a short imperative sentence without a trailing period
- Examples:

```text
feat(parser): support LFU stats extraction
fix(report): correct memory flame graph color legend
chore(ci): bump actions/checkout to v4
```

### Version Consistency Check

- Use `just verify_version` to ensure the current Git tag matches the project's version metadata.
- If no tag is provided, the command auto-detects the tag of the current HEAD.
- If HEAD is not tagged, the command fails and asks you to specify a tag.

Examples:

```bash
# Auto-detect tag from HEAD
just verify_version

# Specify tag explicitly
just verify_version tag=v1.2.3
```

### GitHub Actions dependencies

- Install the official lockfile tool with `gh extension install github/gh-actions-lock`.
- After adding or changing Action references, run `gh actions-lock --no-narrow` from the repository root and review both workflow and lockfile changes. Never edit `.github/workflows/actions.lock` by hand.
- Keep compatible major references such as `@v1`; toolchain and installer channel references retain their existing meaning. The generator migrates same-repository Actions to `$/` references resolved at the running commit.
- Run `just verify_actions` for an offline coverage check, or `gh actions-lock --verify` for the online validation also performed by CI.
- During weekly dependency maintenance, review upstream changes to moving references and refresh approved pins with `gh actions-lock --relock --no-narrow`. Dependabot updates existing locks when proposing Action version changes; it does not onboard new workflows.

### Tests & Coverage

- Unit tests: use `cargo nextest` (via `just test`).
- Coverage: generate a report with `just coverage` and open `target/coverage/html/index.html` locally.
- If you add a new feature or fix a bug, add or update tests accordingly.

### Local Development Environment (Optional)

- Use Docker to start local dependencies:

```bash
just up_dev    # start
just down_dev  # stop
```

- Demo workflow:

```bash
just demo
```


