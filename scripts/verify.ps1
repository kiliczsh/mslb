# Docker-only pre-commit gate. Invoke with PowerShell from any working directory.
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

function Invoke-CheckedDocker {
    param([Parameter(Mandatory)][string[]]$DockerArguments)
    Write-Host ('Running: docker ' + ($DockerArguments -join ' '))
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
        $_ -match '(?i)(^|\s)(warning|warn)(:|\s)|\bdeprecated\b|\bfound\s+[1-9][0-9]*\s+warnings?\b'
    }
    if ($diagnostics) {
        throw ('Warnings must be resolved before committing: ' + ($diagnostics -join "`n"))
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
    foreach ($service in @('python-checks', 'frontend-checks')) {
        Invoke-CheckedDocker -DockerArguments @(
            'compose', '--progress', 'plain', '--profile', 'tools', 'build', $service
        )
    }
    Invoke-CheckedDocker -DockerArguments @('compose', 'up', '-d', '--wait')
    foreach ($service in @('dependencies-check', 'python-checks', 'frontend-checks')) {
        Invoke-CheckedDocker -DockerArguments @(
            'compose', '--profile', 'tools', 'run', '--rm', $service
        )
    }
    Invoke-CheckedDocker -DockerArguments @(
        'compose', '--profile', 'tools', 'run', '--rm', 'bundle-runner'
    )
    Invoke-CheckedDocker -DockerArguments @(
        'compose', '--profile', 'tools', 'run', '--rm', 'bundle-runner',
        'verify', '/data/exports/scaled-example'
    )
    Write-Host 'Docker builds and all implemented checks passed without warnings.'
}
finally {
    Pop-Location
    [Environment]::SetEnvironmentVariable('GIT_CONFIG_COUNT', $previousCount, 'Process')
    [Environment]::SetEnvironmentVariable($keyName, $previousKey, 'Process')
    [Environment]::SetEnvironmentVariable($valueName, $previousValue, 'Process')
}
