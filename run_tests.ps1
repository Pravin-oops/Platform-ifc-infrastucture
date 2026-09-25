<#
.SYNOPSIS
    Test runner for the IFC trigger connector (Windows PowerShell).

.DESCRIPTION
    Runs the unit tests, then exercises the three entry points on the paths that
    need no network. Nothing here touches AWS, BSP, CSM, BAM or a Kafka broker:
    every check runs against the bundled samples and the bundled schemas, so it
    is safe on a laptop and in CI without credentials.

.EXAMPLE
    .\run_tests.ps1
.EXAMPLE
    .\run_tests.ps1 -Coverage
.EXAMPLE
    .\run_tests.ps1 -NoInstall -PytestArgs '-k','envelope'
#>

[CmdletBinding()]
param(
    [switch] $NoInstall,
    [switch] $Coverage,
    [string[]] $PytestArgs = @()
)

$ErrorActionPreference = 'Stop'

$AppDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $AppDir
$VenvDir  = if ($env:IFC_VENV) { $env:IFC_VENV } else { Join-Path $AppDir '.venv' }

function Write-Section([string] $Text) {
    Write-Host ''
    Write-Host "== $Text" -ForegroundColor Cyan
}

function Invoke-Step([string] $Label, [scriptblock] $Body) {
    & $Body
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $Label (exit $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

# ---------------------------------------------------------------------------
# Interpreter
# ---------------------------------------------------------------------------

$VenvPython = Join-Path $VenvDir 'Scripts\python.exe'

if (Test-Path $VenvPython) {
    $Py = $VenvPython
}
elseif (-not $NoInstall) {
    Write-Section "Creating virtualenv at $VenvDir"
    $Bootstrap = if ($env:PYTHON) { $env:PYTHON } else { 'python' }
    & $Bootstrap -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { Write-Host 'Could not create the virtualenv.' -ForegroundColor Red; exit 1 }
    $Py = $VenvPython
}
else {
    $Py = if ($env:PYTHON) { $env:PYTHON } else { 'python' }
}

Write-Section 'Interpreter'
Invoke-Step 'python --version' { & $Py --version }

# ---------------------------------------------------------------------------
# Dependencies
#
# These install an explicit list rather than utility\requirements.txt: the BSP
# client resolves only from the Barclays internal index, and confluent-kafka is
# only needed to talk to a real broker. Neither is required by these tests.
# ---------------------------------------------------------------------------

if (-not $NoInstall) {
    Write-Section 'Installing test dependencies'
    Invoke-Step 'pip upgrade' { & $Py -m pip install --quiet --upgrade pip }
    Invoke-Step 'pip install' {
        & $Py -m pip install --quiet pytest pytest-cov PyYAML fastavro boto3 moto pydantic requests
    }
}

# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

Push-Location $AppDir
try {
    $env:PYTHONPATH = if ($env:PYTHONPATH) { "$RepoRoot;$env:PYTHONPATH" } else { $RepoRoot }

    Write-Section 'Unit tests'
    if ($Coverage) {
        Invoke-Step 'pytest --cov' { & $Py -m pytest -q --cov --cov-report=term-missing @PytestArgs }
    }
    else {
        Invoke-Step 'pytest' { & $Py -m pytest -q @PytestArgs }
    }

    # -----------------------------------------------------------------------
    # Smoke checks: the three entry points, offline.
    # -----------------------------------------------------------------------

    Write-Section 'Smoke: failure catalogue (scripts\main.py catalogue)'
    Invoke-Step 'catalogue' { & $Py scripts\main.py catalogue | Out-Null }
    Write-Host 'catalogue rendered as JSON'

    Write-Section 'Smoke: offline envelope validation (scripts\main.py validate)'
    Invoke-Step 'validate' { & $Py scripts\main.py validate --input samples\trigger_events.jsonl }

    Write-Section 'Smoke: local dry run (scripts\main_local.py --dry-run)'
    Invoke-Step 'dry run' { & $Py scripts\main_local.py --dry-run --log-level WARNING }

    Write-Section 'Smoke: ECS entry point refuses to start without a config'
    # Run detached: this step expects a non-zero exit and output on stderr, and
    # in Windows PowerShell 5.1 redirecting a native command's stderr inline
    # raises NativeCommandError instead of just capturing the text.
    $StdOut = [System.IO.Path]::GetTempFileName()
    $StdErr = [System.IO.Path]::GetTempFileName()
    try {
        $Proc = Start-Process -FilePath $Py `
                              -ArgumentList 'scripts\main_ecs.py' `
                              -WorkingDirectory $AppDir `
                              -NoNewWindow -Wait -PassThru `
                              -RedirectStandardOutput $StdOut `
                              -RedirectStandardError $StdErr
        if ($Proc.ExitCode -eq 0) {
            Write-Host 'FAIL: main_ecs.py exited 0 with no APP_CONFIG_PATH' -ForegroundColor Red
            exit 1
        }
        Write-Host "main_ecs.py exited $($Proc.ExitCode) as expected"
    }
    finally {
        Remove-Item $StdOut, $StdErr -ErrorAction SilentlyContinue
    }

    Write-Host ''
    Write-Host 'All checks passed.' -ForegroundColor Green
}
finally {
    Pop-Location
}
