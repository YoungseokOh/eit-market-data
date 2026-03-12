param(
    [int]$Timeout = 600,
    [string]$Ticker = "005930",
    [string]$Start = "2026-03-01",
    [string]$End = "2026-03-12",
    [string]$Market = "KRX",
    [switch]$SkipLogin,
    [switch]$ForceLogin
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

function Get-BasePython {
    foreach ($CandidateCommand in @(
        @("py", "-3"),
        @("python"),
        @("python3")
    )) {
        $Exe = $CandidateCommand[0]
        $Args = @()
        if ($CandidateCommand.Length -gt 1) {
            $Args = $CandidateCommand[1..($CandidateCommand.Length - 1)]
        }
        if (-not (Get-Command $Exe -ErrorAction SilentlyContinue)) {
            continue
        }

        try {
            & $Exe @Args --version *> $null
            if ($LASTEXITCODE -eq 0) {
                return ,$CandidateCommand
            }
        }
        catch {
            continue
        }
    }

    throw (
        "Python 3.11+ was not found. Install Python from python.org with PATH enabled, or install the Python Launcher " +
        "for Windows so 'py -3' is available."
    )
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Command
    )

    $Exe = $Command[0]
    $Args = @()
    if ($Command.Length -gt 1) {
        $Args = $Command[1..($Command.Length - 1)]
    }

    Write-Host ">> $($Command -join ' ')" -ForegroundColor Cyan
    & $Exe @Args
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $($Command -join ' ')"
    }
}

$BasePython = @(Get-BasePython)
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$VenvPlaywright = Join-Path $RepoRoot ".venv\Scripts\playwright.exe"

if (-not (Test-Path $VenvPython)) {
    Invoke-Checked -Command ($BasePython + @("-m", "venv", ".venv"))
}

Invoke-Checked @($VenvPython, "-m", "pip", "install", "--upgrade", "pip")
Invoke-Checked @($VenvPython, "-m", "pip", "install", "-e", ".[kr]")
Invoke-Checked @($VenvPlaywright, "install", "chromium")

if (-not $SkipLogin) {
    $LoginArgs = @($VenvPython, "scripts/krx_login.py", "--timeout", "$Timeout")
    if ($ForceLogin) {
        $LoginArgs += "--force"
    }
    Invoke-Checked $LoginArgs
}

Invoke-Checked @(
    $VenvPython,
    "scripts/probe_fdr_krx_session.py",
    "--ticker",
    $Ticker,
    "--start",
    $Start,
    "--end",
    $End,
    "--market",
    $Market
)

$CookiePath = & $VenvPython -c "from eit_market_data.kr.krx_auth import resolve_cookie_path; print(resolve_cookie_path())"

Write-Host ""
Write-Host "KRX cookies:" -ForegroundColor Green
Write-Host $CookiePath.Trim()
