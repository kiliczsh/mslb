# Docker-only pre-commit gate. Invoke with PowerShell from any working directory.
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$verificationSteps = [System.Collections.Generic.List[object]]::new()
$verificationTimer = [System.Diagnostics.Stopwatch]::StartNew()
$verificationPassed = $false
$timingDirectory = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../.cache/verification'))

function Invoke-TimedCheck {
    param([Parameter(Mandatory)][string]$Name, [Parameter(Mandatory)][scriptblock]$Action)
    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    $passed = $false
    try {
        & $Action
        $passed = $true
    }
    finally {
        $timer.Stop()
        $verificationSteps.Add([ordered]@{
            name = $Name
            seconds = [Math]::Round($timer.Elapsed.TotalSeconds, 3)
            passed = $passed
        })
        Write-Host ('Elapsed: {0:N2}s ({1})' -f $timer.Elapsed.TotalSeconds, $Name)
    }
}

function Invoke-CheckedDocker {
    param([Parameter(Mandatory)][string[]]$DockerArguments)
    Write-Host ('Running: docker ' + ($DockerArguments -join ' '))
    Invoke-TimedCheck -Name ('docker ' + ($DockerArguments -join ' ')) -Action {
        $lines = [System.Collections.Generic.List[string]]::new()
        & docker @DockerArguments 2>&1 | ForEach-Object {
            $line = [regex]::Replace($_.ToString(), '\x1B\[[0-?]*[ -/]*[@-~]', '')
            $lines.Add($line)
            Write-Host $line
        }
        if ($LASTEXITCODE -ne 0) {
            throw "Docker command failed with exit code $LASTEXITCODE"
        }
        $diagnostics = $lines | Where-Object {
            $_ -match '(?i)(^|[\s=])(warning|warn)(:|\s)|\bdeprecated\b|\bfound\s+[1-9][0-9]*\s+warnings?\b'
        }
        if ($diagnostics) {
            throw ('Warnings must be resolved before committing: ' + ($diagnostics -join "`n"))
        }
    }
}

# BuildKit reads Git metadata as the desktop user. Trust only this checkout for
# this process and its children; do not change the user's global Git settings.
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path.Replace('\', '/')
$previousCount = [Environment]::GetEnvironmentVariable('GIT_CONFIG_COUNT', 'Process')
$configIndex = if ([string]::IsNullOrEmpty($previousCount)) { 0 } else { [int]$previousCount }
$keyName = "GIT_CONFIG_KEY_$configIndex"
$valueName = "GIT_CONFIG_VALUE_$configIndex"
$previousKey = [Environment]::GetEnvironmentVariable($keyName, 'Process')
$previousValue = [Environment]::GetEnvironmentVariable($valueName, 'Process')

Push-Location (Join-Path $PSScriptRoot '..')
try {
    [Environment]::SetEnvironmentVariable('GIT_CONFIG_COUNT', ($configIndex + 1).ToString(), 'Process')
    [Environment]::SetEnvironmentVariable($keyName, 'safe.directory', 'Process')
    [Environment]::SetEnvironmentVariable($valueName, $repositoryRoot, 'Process')
    Invoke-CheckedDocker -DockerArguments @('compose', '--profile', 'tools', 'config', '--quiet')
    foreach ($service in @('postgres', 'python-checks', 'api', 'frontend-checks', 'web', 'browser-checks')) {
        Invoke-CheckedDocker -DockerArguments @(
            'compose', '--progress', 'plain', '--profile', 'tools', 'build', $service
        )
    }
    Invoke-TimedCheck -Name 'Effective deployment preflight' -Action {
        $effectiveConfig = & docker compose --profile tools config --format json
        if ($LASTEXITCODE -ne 0) { throw 'Compose resolution failed' }
        $preflightOutput = $effectiveConfig | & docker run --rm -i --network=none --memory=128m --cpus=1 --pids-limit=32 mslb-python-tools:local python scripts/check_deployment.py 2>&1
        $preflightExit = $LASTEXITCODE
        $preflightOutput | ForEach-Object { Write-Host $_ }
        if ($preflightExit -ne 0) { throw 'Effective deployment preflight failed' }
    }
    # A rebuilt tag can have a new immutable manifest identity while Compose keeps
    # cached configuration labels. Refresh only this project's services before
    # testing enrollment against the newly built runtime image.
    Invoke-CheckedDocker -DockerArguments @(
        'compose', 'up', '--no-build', '--force-recreate', '-d', '--wait'
    )
    Invoke-TimedCheck -Name 'Host loopback API and web connectivity' -Action {
        $hostHealth = Invoke-WebRequest -Uri 'http://127.0.0.1:8000/api/v1/health/live' -TimeoutSec 5
        if ($hostHealth.StatusCode -ne 200) { throw 'Host loopback API health failed' }
        Write-Host 'Host loopback API connectivity verified'
        $webHealth = Invoke-WebRequest -Uri 'http://127.0.0.1:8080/health' -TimeoutSec 5
        if ($webHealth.StatusCode -ne 200) { throw 'Host loopback dashboard health failed' }
        Write-Host 'Host loopback dashboard connectivity verified'
    }
    foreach ($service in @('dependencies-check', 'migrate', 'runtime-check', 'python-checks', 'persistence-checks', 'frontend-checks', 'worker-check', 'api-check', 'mcp-check', 'traffic-check', 'docker-proxy-check', 'docker-public-check')) {
        Invoke-CheckedDocker -DockerArguments @(
            'compose', '--profile', 'tools', 'run', '--rm', '--no-deps', $service
        )
    }
    Invoke-TimedCheck -Name 'Project controller/backend/control outage recovery' -Action {
        & (Join-Path $PSScriptRoot 'check-controller-faults.ps1')
    }
    Invoke-CheckedDocker -DockerArguments @(
        'compose', '--profile', 'tools', 'run', '--rm', '--no-deps', 'output-maintenance'
    )
    # Redis is only a wake-up/cache hint. Exercise actual guarded durable work during
    # its outage, and always restore this project's service before leaving the gate.
    try {
        Invoke-CheckedDocker -DockerArguments @('compose', 'stop', 'redis')
        foreach ($service in @('worker-check', 'api-check')) {
            Invoke-CheckedDocker -DockerArguments @(
                'compose', '--profile', 'tools', 'run', '--rm', '--no-deps', $service
            )
        }
    } finally {
        Invoke-CheckedDocker -DockerArguments @('compose', 'up', '--no-build', '-d', '--wait', 'redis')
    }
    Invoke-CheckedDocker -DockerArguments @(
        'compose', '--profile', 'tools', 'run', '--rm', '--no-deps', 'bundle-runner'
    )
    Invoke-CheckedDocker -DockerArguments @(
        'compose', '--profile', 'tools', 'run', '--rm', '--no-deps', 'bundle-runner',
        'verify', '/data/exports/scaled-example'
    )
    # Browser acceptance runs last, after all lower-level and recovery checks.
    Invoke-CheckedDocker -DockerArguments @(
        'compose', '--profile', 'tools', 'run', '--rm', '--no-deps',
        '-e', 'BROWSER_TEST_INSTALLATION=guarded', 'browser-checks',
        'pnpm', 'test:browser', 'browser/docker.spec.ts'
    )
    Invoke-TimedCheck -Name 'Isolated browser installation, tests and cleanup' -Action {
        & (Join-Path $PSScriptRoot 'check-browser.ps1')
    }
    Write-Host 'Docker builds and all implemented checks passed without warnings.'
    $verificationPassed = $true
}
finally {
    $verificationTimer.Stop()
    Pop-Location
    [Environment]::SetEnvironmentVariable('GIT_CONFIG_COUNT', $previousCount, 'Process')
    [Environment]::SetEnvironmentVariable($keyName, $previousKey, 'Process')
    [Environment]::SetEnvironmentVariable($valueName, $previousValue, 'Process')
    New-Item -ItemType Directory -Force -Path $timingDirectory | Out-Null
    [ordered]@{
        completed_at = [DateTimeOffset]::UtcNow.ToString('o')
        passed = $verificationPassed
        elapsed_seconds = [Math]::Round($verificationTimer.Elapsed.TotalSeconds, 3)
        steps = $verificationSteps.ToArray()
    } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $timingDirectory 'last-run.json') -Encoding utf8
    Write-Host ('Verification elapsed: {0:N2}s' -f $verificationTimer.Elapsed.TotalSeconds)
}
