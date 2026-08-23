#!/usr/bin/env bash
#
# Entrypoint.sh
#
# Dispatches a container invocation to one stage of the bronze -> silver ->
# gold pipeline. This is the container-native equivalent of
# pipeline/run_pipeline.ps1 - same stages, same order, but designed so each
# stage can also run as its own short-lived container (see compose.yml).
#
# Usage (as the image's ENTRYPOINT, so these are just `docker run <image> ...`):
#   bronze [postgres|mongo|both]        Bronze incremental (append-only) load
#   dbt-run   <layer> [extra dbt args]  dbt run   --select <layer>
#   dbt-test  <layer> [extra dbt args]  dbt test  --select <layer>
#   sql-test  <layer> [extra args]      SQL data-quality tests for <layer>
#   full                                Every stage above, in order, bronze -> gold
#   <anything else>                     Executed as-is (e.g. `bash` to get a shell)
#
# Env vars:
#   DBT_PROJECT_DIR   Path to the folder containing dbt_project.yml.
#                      Auto-detected under PROJECT_ROOT if unset.
#   PROJECT_ROOT       Defaults to /app (where the Dockerfile COPYs the repo).
#   BRONZE_SOURCE      Default --source for `full` and bare `bronze` calls (both).
#
# Author: Nitin
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/app}"
SCRIPTS_DIR="${PROJECT_ROOT}/scripts"

log() {
    printf '[entrypoint] %s %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

find_dbt_project_dir() {
    if [ -n "${DBT_PROJECT_DIR:-}" ]; then
        printf '%s' "$DBT_PROJECT_DIR"
        return 0
    fi
    # Same idea as run_dbt.py's discover_dbt_project_dir(): look for
    # dbt_project.yml at the project root or one level below it.
    local found
    found="$(find "$PROJECT_ROOT" -maxdepth 2 -name dbt_project.yml -print -quit 2>/dev/null || true)"
    if [ -n "$found" ]; then
        dirname "$found"
    fi
}

run_bronze() {
    local source="${1:-${BRONZE_SOURCE:-both}}"
    log "Bronze incremental load (source=${source})"
    cd "$SCRIPTS_DIR"
    uv run bronze_pipeline.py --source "$source"
}

run_dbt_step() {
    local action="$1" layer="$2"
    shift 2 || true
    local dbt_dir
    dbt_dir="$(find_dbt_project_dir)"
    if [ -z "$dbt_dir" ]; then
        log "ERROR: could not locate dbt_project.yml under ${PROJECT_ROOT}. Set DBT_PROJECT_DIR explicitly."
        exit 1
    fi
    log "dbt ${action} --select ${layer} (project: ${dbt_dir}) extra_args=[$*]"
    cd "$dbt_dir"
    dbt "$action" --select "$layer" "$@"
}

run_sql_test() {
    local layer="$1"
    shift || true
    log "SQL data-quality tests for ${layer} layer"
    cd "$SCRIPTS_DIR"
    uv run databricks_test.py --layer "$layer" "$@"
}

run_full() {
    run_bronze "${BRONZE_SOURCE:-both}"
    run_dbt_step run silver
    run_dbt_step test silver
    run_sql_test silver
    run_dbt_step run gold
    run_dbt_step test gold
    run_sql_test gold
    log "Full pipeline: bronze -> silver -> gold completed successfully."
}

command="${1:-}"
if [ $# -gt 0 ]; then
    shift
fi

case "$command" in
    bronze)
        run_bronze "$@"
        ;;
    dbt-run)
        [ $# -ge 1 ] || { log "ERROR: dbt-run requires a layer, e.g. 'dbt-run silver'"; exit 1; }
        run_dbt_step run "$@"
        ;;
    dbt-test)
        [ $# -ge 1 ] || { log "ERROR: dbt-test requires a layer, e.g. 'dbt-test silver'"; exit 1; }
        run_dbt_step test "$@"
        ;;
    sql-test)
        [ $# -ge 1 ] || { log "ERROR: sql-test requires a layer, e.g. 'sql-test silver'"; exit 1; }
        run_sql_test "$@"
        ;;
    full)
        run_full
        ;;
    "")
        log "No command given."
        log "Usage: bronze [source] | dbt-run <layer> [args] | dbt-test <layer> [args] | sql-test <layer> [args] | full"
        exit 1
        ;;
    *)
        # Anything else (bash, sh, python, a one-off debugging command, ...)
        # is executed directly so the image stays useful for shelling in.
        log "Executing: ${command} $*"
        exec "$command" "$@"
        ;;
esac