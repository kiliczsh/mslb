# Run browser acceptance only against uniquely owned disposable Docker resources.
[CmdletBinding()]
param([string]$Grep = '')

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

function Invoke-BrowserDocker {
    param([Parameter(Mandatory)][string[]]$DockerArguments)
    Write-Host ('Running: docker ' + ($DockerArguments -join ' '))
    $lines = [System.Collections.Generic.List[string]]::new()
    & docker @DockerArguments 2>&1 | ForEach-Object {
        $line = [regex]::Replace($_.ToString(), '\x1B\[[0-?]*[ -/]*[@-~]', '')
        $lines.Add($line)
        Write-Host $line
    }
    if ($LASTEXITCODE -ne 0) { throw "Browser Docker command failed with exit code $LASTEXITCODE" }
    $diagnostics = $lines | Where-Object {
        $_ -match '(?i)(^|[\s=])(warning|warn)(:|\s)|\bdeprecated\b|\bfound\s+[1-9][0-9]*\s+warnings?\b'
    }
    if ($diagnostics) { throw ('Browser warnings must be resolved: ' + ($diagnostics -join "`n")) }
}

$browserProject = 'mslb-browser-' + [Guid]::NewGuid().ToString('N').Substring(0, 12)
$configuration = (Resolve-Path (Join-Path $PSScriptRoot '../compose.browser-tests.yaml')).Path
$composeArguments = @('compose', '-f', $configuration, '-p', $browserProject)
Invoke-BrowserDocker -DockerArguments ($composeArguments + @('config', '--quiet'))
$resolved = & docker @composeArguments config --format json
if ($LASTEXITCODE -ne 0) { throw 'Browser Compose resolution failed' }
$config = $resolved | ConvertFrom-Json
if ($config.name -ne $browserProject -or $browserProject -notmatch '^mslb-browser-[0-9a-f]{12}$') {
    throw 'Refusing an unowned browser project namespace'
}
foreach ($service in $config.services.PSObject.Properties.Value) {
    if ($service.PSObject.Properties.Name -contains 'ports' -and $service.ports.Count -ne 0) {
        throw 'Browser installation must not publish host ports'
    }
}
foreach ($volume in $config.volumes.PSObject.Properties.Value) {
    if ($volume.name -notlike ($browserProject + '_*') -or
        ($volume.PSObject.Properties.Name -contains 'external' -and $volume.external)) {
        throw 'Browser installation must not reuse external or developer volumes'
    }
}
foreach ($network in $config.networks.PSObject.Properties.Value) {
    if ($network.name -notlike ($browserProject + '_*') -or
        ($network.PSObject.Properties.Name -contains 'external' -and $network.external)) {
        throw 'Browser installation must not reuse developer networks'
    }
}
try {
    Invoke-BrowserDocker -DockerArguments ($composeArguments + @('up', '-d', '--wait', '--no-build', 'web', 'worker'))
    $testArguments = @('run', '--rm', '--no-deps', 'browser-checks')
    if ($Grep) { $testArguments += @('pnpm', 'test:browser', '--grep', $Grep) }
    Invoke-BrowserDocker -DockerArguments ($composeArguments + $testArguments)
}
finally {
    Invoke-BrowserDocker -DockerArguments ($composeArguments + @('down', '--volumes', '--remove-orphans', '--timeout', '10'))
}
Write-Host 'Isolated browser installation verified and removed.'
