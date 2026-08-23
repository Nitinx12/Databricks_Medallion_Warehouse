<#
.SYNOPSIS
    Orchestrates the full Bronze -> Silver -> Gold pipeline end to end.

.DESCRIPTION
    Runs, in order:
        1. Bronze incremental load           scripts\bronze_pipeline.py
        2. dbt run   --select silver
        3. dbt test  --select silver
        4. SQL data-quality tests - silver   scripts\databricks_test.py --layer silver
        5. dbt run   --select gold
        6. dbt test  --select gold
        7. SQL data-quality tests - gold     scripts\databricks_test.py --layer gold

    Stops at the first failing step by default (remaining steps are marked
    SKIPPED). Use -ContinueOnError to run every step regardless.

    Expected project layout (this script lives in pipeline\):

        project_root\
        |-- pipeline\
        |   `-- run_pipeline.ps1        <- this script
        |-- scripts\
        |   |-- bronze_pipeline.py
        |   |-- databricks_test.py
        |   `-- run_dbt.py
        |-- utils\
        |-- tests\
        |   |-- bronze\
        |   |-- silver\
        |   `-- gold\
        `-- DBT_databricks\  (or similar folder containing dbt_project.yml)

.PARAMETER ProjectDir
    Path to the dbt project (the folder containing dbt_project.yml). If
    omitted, it is auto-detected by searching the project root and its
    immediate subfolders.

.PARAMETER BronzeSource
    Passed straight through to bronze_pipeline.py --source.
    One of: postgres, mongo, both. Default: both.

.PARAMETER SkipBronze
    Skip the bronze load step and start straight from the silver dbt build.

.PARAMETER FullRefresh
    Pass --full-refresh to both `dbt run` steps.

.PARAMETER FailFastSql
    Pass --fail-fast to the SQL data-quality test steps (databricks_test.py),
    so a single failing .sql test stops that layer's test run immediately.

.PARAMETER ContinueOnError
    Run every step even if an earlier one fails. Default: stop at the first
    failure and mark the rest SKIPPED.

.PARAMETER JsonReportDir
    If set, each SQL data-quality step also writes a JSON report here
    (silver_report.json / gold_report.json).

.PARAMETER ExtraDbtArgs
    Extra arguments appended verbatim to every `dbt run` / `dbt test` call,
    e.g. -ExtraDbtArgs "--target","prod".

.PARAMETER DryRun
    Print the steps and commands that would run, without executing anything.

.EXAMPLE
    .\run_pipeline.ps1

.EXAMPLE
    .\run_pipeline.ps1 -SkipBronze -FullRefresh

.EXAMPLE
    .\run_pipeline.ps1 -DryRun

.EXAMPLE
    .\run_pipeline.ps1 -ContinueOnError -FailFastSql -JsonReportDir .\reports

.NOTES
    Author: Nitin
    Exit code is 0 only when every step that ran completed successfully.
#>

[CmdletBinding()]
param(
    [string]$ProjectDir,

    [ValidateSet("postgres", "mongo", "both")]
    [string]$BronzeSource = "both",

    [switch]$SkipBronze,
    [switch]$FullRefresh,
    [switch]$FailFastSql,
    [switch]$ContinueOnError,
    [switch]$DryRun,

    [string]$JsonReportDir,

    [string[]]$ExtraDbtArgs = @()
)

$ErrorActionPreference = "Stop"

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
$PipelineDir = $PSScriptRoot
$ProjectRoot = Split-Path -Parent $PipelineDir
$ScriptsDir  = Join-Path $ProjectRoot "scripts"

$BronzeScript  = "bronze_pipeline.py"
$SqlTestScript = "databricks_test.py"

foreach ($required in @(
        $ScriptsDir,
        (Join-Path $ScriptsDir $BronzeScript),
        (Join-Path $ScriptsDir $SqlTestScript)
    )) {
    if (-not (Test-Path $required)) {
        Write-Error "Expected path not found: $required`nRun this script from pipeline\ inside the project root, next to scripts\."
        exit 1
    }
}

# --------------------------------------------------------------------------
# dbt project auto-detection (mirrors run_dbt.py's discover_dbt_project_dir)
# --------------------------------------------------------------------------
function Find-DbtProjectDir {
    param([string]$Root)

    if (Test-Path (Join-Path $Root "dbt_project.yml")) {
        return $Root
    }
    $candidates = Get-ChildItem -Path $Root -Directory -ErrorAction SilentlyContinue |
        Where-Object { Test-Path (Join-Path $_.FullName "dbt_project.yml") } |
        Sort-Object -Property @{Expression = { $_.Name -notmatch "dbt" } }, Name
    if ($candidates) {
        return $candidates[0].FullName
    }
    return $null
}

if (-not $ProjectDir) {
    $ProjectDir = Find-DbtProjectDir -Root $ProjectRoot
    if (-not $ProjectDir) {
        Write-Error "Could not auto-detect a dbt project under $ProjectRoot (no dbt_project.yml found in it or its immediate subfolders). Pass -ProjectDir explicitly, e.g. -ProjectDir .\DBT_databricks"
        exit 1
    }
}
elseif (-not (Test-Path (Join-Path $ProjectDir "dbt_project.yml"))) {
    Write-Error "-ProjectDir does not contain a dbt_project.yml: $ProjectDir"
    exit 1
}
else {
    $ProjectDir = (Resolve-Path $ProjectDir).Path
}

# --------------------------------------------------------------------------
# Preflight tool checks
# --------------------------------------------------------------------------
if (-not $DryRun) {
    if (-not (Get-Command dbt -ErrorAction SilentlyContinue)) {
        Write-Error "'dbt' was not found on PATH. Activate the environment that has dbt-core (and your adapter) installed, then retry."
        exit 1
    }
    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        Write-Error "'uv' was not found on PATH."
        exit 1
    }
}

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
$Script:Results = New-Object System.Collections.Generic.List[object]

function Write-StepHeader {
    param([string]$Name)
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
    Write-Host " $Name" -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor DarkCyan
}

function Invoke-Step {
    param(
        [Parameter(Mandatory)] [string]$Name,
        [Parameter(Mandatory)] [string]$Exe,
        [Parameter(Mandatory)] [string[]]$Args,
        [Parameter(Mandatory)] [string]$WorkingDirectory
    )

    Write-StepHeader -Name $Name
    Write-Host "`$ $Exe $($Args -join ' ')" -ForegroundColor DarkGray
    Write-Host "(cwd: $WorkingDirectory)`n" -ForegroundColor DarkGray

    if ($DryRun) {
        $Script:Results.Add([PSCustomObject]@{ Step = $Name; Status = "DRY-RUN"; Duration = "-"; ExitCode = "-" })
        return $true
    }

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    Push-Location $WorkingDirectory
    try {
        & $Exe @Args
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    $stopwatch.Stop()

    $passed = ($exitCode -eq 0)
    $Script:Results.Add([PSCustomObject]@{
            Step     = $Name
            Status   = if ($passed) { "PASS" } else { "FAIL" }
            Duration = "{0:N1}s" -f $stopwatch.Elapsed.TotalSeconds
            ExitCode = $exitCode
        })

    if ($passed) {
        Write-Host "`n[$Name] PASS ($('{0:N1}' -f $stopwatch.Elapsed.TotalSeconds)s)" -ForegroundColor Green
    }
    else {
        Write-Host "`n[$Name] FAIL (exit code $exitCode)" -ForegroundColor Red
    }
    return $passed
}

function Add-SkippedStep {
    param([string]$Name)
    $Script:Results.Add([PSCustomObject]@{ Step = $Name; Status = "SKIPPED"; Duration = "-"; ExitCode = "-" })
}

function Get-SqlTestArgs {
    param([string]$Layer)

    $reportArgs = @()
    if ($JsonReportDir) {
        if (-not (Test-Path $JsonReportDir)) {
            New-Item -ItemType Directory -Path $JsonReportDir -Force | Out-Null
        }
        $reportPath = Join-Path (Resolve-Path $JsonReportDir).Path "$Layer`_report.json"
        $reportArgs = @("--json-report", $reportPath)
    }
    $fastArgs = @()
    if ($FailFastSql) { $fastArgs = @("--fail-fast") }

    return @("run", $SqlTestScript, "--layer", $Layer) + $fastArgs + $reportArgs
}

# --------------------------------------------------------------------------
# Build the ordered step list
# --------------------------------------------------------------------------
$fullRefreshArgs = @()
if ($FullRefresh) { $fullRefreshArgs = @("--full-refresh") }

$steps = New-Object System.Collections.Generic.List[object]

if (-not $SkipBronze) {
    $steps.Add([PSCustomObject]@{
            Name = "Bronze incremental load"
            Exe  = "uv"
            Args = @("run", $BronzeScript, "--source", $BronzeSource)
            Cwd  = $ScriptsDir
        })
}

$steps.Add([PSCustomObject]@{
        Name = "Build silver models (dbt run)"
        Exe  = "dbt"
        Args = @("run", "--select", "silver") + $fullRefreshArgs + $ExtraDbtArgs
        Cwd  = $ProjectDir
    })
$steps.Add([PSCustomObject]@{
        Name = "Test silver models (dbt test)"
        Exe  = "dbt"
        Args = @("test", "--select", "silver") + $ExtraDbtArgs
        Cwd  = $ProjectDir
    })
$steps.Add([PSCustomObject]@{
        Name = "Silver SQL data-quality tests"
        Exe  = "uv"
        Args = Get-SqlTestArgs -Layer "silver"
        Cwd  = $ScriptsDir
    })
$steps.Add([PSCustomObject]@{
        Name = "Build gold models (dbt run)"
        Exe  = "dbt"
        Args = @("run", "--select", "gold") + $fullRefreshArgs + $ExtraDbtArgs
        Cwd  = $ProjectDir
    })
$steps.Add([PSCustomObject]@{
        Name = "Test gold models (dbt test)"
        Exe  = "dbt"
        Args = @("test", "--select", "gold") + $ExtraDbtArgs
        Cwd  = $ProjectDir
    })
$steps.Add([PSCustomObject]@{
        Name = "Gold SQL data-quality tests"
        Exe  = "uv"
        Args = Get-SqlTestArgs -Layer "gold"
        Cwd  = $ScriptsDir
    })

# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------
Write-Host ("#" * 78) -ForegroundColor Blue
Write-Host "# Bronze -> Silver -> Gold pipeline" -ForegroundColor Blue
Write-Host "# project root : $ProjectRoot" -ForegroundColor Blue
Write-Host "# dbt project  : $ProjectDir" -ForegroundColor Blue
if ($DryRun) { Write-Host "# mode         : DRY RUN (nothing will execute)" -ForegroundColor Blue }
Write-Host ("#" * 78) -ForegroundColor Blue

$stopAll = $false
foreach ($step in $steps) {
    if ($stopAll) {
        Add-SkippedStep -Name $step.Name
        continue
    }
    $ok = Invoke-Step -Name $step.Name -Exe $step.Exe -Args $step.Args -WorkingDirectory $step.Cwd
    if (-not $ok -and -not $DryRun -and -not $ContinueOnError) {
        Write-Host "`nStopping pipeline: '$($step.Name)' failed. Pass -ContinueOnError to run remaining steps anyway.`n" -ForegroundColor Red
        $stopAll = $true
    }
}

# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------
Write-Host ""
Write-Host ("=" * 78) -ForegroundColor DarkCyan
Write-Host " Pipeline summary" -ForegroundColor Cyan
Write-Host ("=" * 78) -ForegroundColor DarkCyan
$Script:Results | Format-Table -AutoSize | Out-String | Write-Host

if ($DryRun) {
    Write-Host "--dry-run: no commands were executed." -ForegroundColor Yellow
    exit 0
}

$failed = $Script:Results | Where-Object { $_.Status -eq "FAIL" }
if ($failed) {
    Write-Host "Pipeline FAILED - $($failed.Count) step(s) failed." -ForegroundColor Red
    exit 1
}
else {
    Write-Host "Pipeline PASSED - all steps completed successfully." -ForegroundColor Green
    exit 0
}