# AGENTS.md

Instructions for Codex when working in this repository. Read this file fully before making any change.

## Project Overview

Data engineering and analytics project. Stack typically includes SQL (PostgreSQL), Python, dbt, PySpark, Airflow, and PowerShell for orchestration on Windows. Update this section with the actual project name and purpose.

## Naming Conventions

- Use snake_case for everything unless the language or tool forces otherwise:
  - Python: variables, functions, files, modules -> snake_case (e.g. `load_customer_data.py`, `def transform_orders():`)
  - SQL: tables, columns, CTEs, stored procedures -> snake_case (e.g. `fct_orders`, `customer_id`, `sp_refresh_summary`)
  - dbt models: snake_case, prefixed by layer (`stg_`, `int_`, `fct_`, `dim_`)
  - PowerShell: function names use approved verbs in PascalCase per convention (e.g. `Invoke-Pipeline`), but variables and parameters inside scripts stay snake_case where practical
  - Environment variables and config keys: SCREAMING_SNAKE_CASE
- No camelCase, no kebab case in code identifiers. Kebab case is fine only in file paths or CLI flags where the tool requires it.
- File names: snake_case, lowercase, no spaces.

## Comments

- Keep comments short and meaningful. One line explaining why, not what.
- Do not comment on obvious code. Do not narrate every line.
- Every comment should answer "why this exists" or "why this approach", not restate the code.
- Bad: `# increment counter`
- Good: `# skip weekends since source system does not post on Sat or Sun`
- Docstrings only for public functions and modules, kept to two or three lines.
- No decorative comment blocks or ASCII banners.

## Documentation Style

- No hyphens in prose text inside markdown files, READMEs, or docstrings. Hyphens are fine inside code syntax, flags, and file names.
- Prefer plain, direct sentences over long paragraphs.
- Data catalogs, architecture docs, and READMEs should be updated when a change affects them, not left stale.

## Git Workflow

This is the most important section. Follow it exactly.

- Never stage everything with `git add .` or `git add -A` unless explicitly told to.
- Stage and commit files one at a time, grouped by logical change, not in one giant commit.
- Workflow per file:
  1. Make the change to a single file.
  2. `git add path/to/file`
  3. `git commit -m "type: short meaningful message"`
  4. Move to the next file.
- If two files are tightly coupled and cannot function independently (e.g. a migration and the model that depends on it), they can be committed together, but this should be the exception, not the default.
- Commit message format: `type: short description`, all lowercase, no period at the end, under 72 characters.
  - Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `style`, `perf`
  - Example: `feat: add incremental load for orders table`
  - Example: `fix: correct integer division truncation in retention query`
  - Example: `docs: update data catalog for fct_orders`
- Do not bundle unrelated changes into one commit even if working on the same file. Split with `git add -p` if needed.
- Never force push. Never rewrite shared history.
- Before committing, run any available linter or formatter for the file type touched (sqlfluff for SQL, ruff or black for Python).

## Code Quality Checks Before Commit

- Python: run `ruff check` and `ruff format` (or `black`) before committing.
- SQL: run `sqlfluff lint` if configured.
- dbt: run `dbt parse` at minimum, `dbt build --select <model>` when feasible, before committing a model change.
- Do not commit code that fails linting without saying so explicitly in the commit message body.

## Testing

- If tests exist, run the relevant test file after a change, not the full suite every time, unless asked.
- Add a test alongside new logic when the pattern already has test coverage elsewhere in the repo.
- Do not delete or weaken a test to make it pass.

## Package Management

- Use `uv` for Python dependency management. Do not introduce pip, poetry, or conda into a project that already uses `uv`.
- Add dependencies with `uv add <package>`, not by hand editing `pyproject.toml` unless necessary.

## What Not To Do

- Do not rename existing files or restructure directories without being asked.
- Do not add new dependencies without checking if an existing one already covers the need.
- Do not write speculative or unused code "for the future".
- Do not touch `.env`, secrets, or credentials files.
- Do not squash or amend commits that have already been described as final in the conversation.

## When Unsure

- If a naming convention, commit boundary, or approach is ambiguous, pick the most conventional option for the language and proceed. State the assumption in the commit message body if it matters.