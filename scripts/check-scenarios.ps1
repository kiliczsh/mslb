# Exercise standard rejection and actual expanded workloads only in fresh owned projects.
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$applicationRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$campaignId = [Guid]::NewGuid().ToString('N')
$campaignRoot = Join-Path $applicationRoot ('.cache/scenarios/' + $campaignId)
[System.IO.Directory]::CreateDirectory($campaignRoot) | Out-Null
$configuration = Join-Path $applicationRoot 'compose.scenario-tests.yaml'
$previousProfile = [Environment]::GetEnvironmentVariable('SCENARIO_CAPACITY_PROFILE', 'Process')
$previousIdentity = [Environment]::GetEnvironmentVariable('SCENARIO_INSTALLATION_ID', 'Process')

function Invoke-ScenarioDocker {
    param([Parameter(Mandatory)][string[]]$DockerArguments)
    & docker @DockerArguments
    if ($LASTEXITCODE -ne 0) { throw "Scenario Docker command failed with exit code $LASTEXITCODE" }
}

try {
    foreach ($capacityProfile in @('standard', 'stress')) {
        $installationId = [Guid]::NewGuid().ToString()
        $project = 'mslb-scenarios-' + $capacityProfile + '-' + $campaignId.Substring(0, 12)
        $runner = $project + '-runner'
        $destination = Join-Path $campaignRoot $capacityProfile
        [System.IO.Directory]::CreateDirectory($destination) | Out-Null
        [Environment]::SetEnvironmentVariable('SCENARIO_CAPACITY_PROFILE', $capacityProfile, 'Process')
        [Environment]::SetEnvironmentVariable('SCENARIO_INSTALLATION_ID', $installationId, 'Process')
        $composeArguments = @('compose', '-f', $configuration, '-p', $project)
        $resolved = & docker @composeArguments config --format json
        if ($LASTEXITCODE -ne 0) { throw 'Scenario Compose resolution failed' }
        $config = $resolved | ConvertFrom-Json
        if ($config.name -ne $project -or $project -notmatch '^mslb-scenarios-(standard|stress)-[0-9a-f]{12}$') {
            throw 'Refusing an unowned scenario project'
        }
        foreach ($service in $config.services.PSObject.Properties.Value) {
            if ($service.PSObject.Properties.Name -contains 'ports' -and $service.ports.Count -ne 0) {
                throw 'Scenario services must not publish host ports'
            }
            if ($service.PSObject.Properties.Name -contains 'volumes') {
                foreach ($mount in $service.volumes) {
                    if ($mount.type -ne 'volume') { throw 'Scenario services require only owned named volumes' }
                }
            }
        }
        foreach ($kind in @('volumes', 'networks')) {
            foreach ($resource in $config.$kind.PSObject.Properties.Value) {
                if ($resource.name -notlike ($project + '_*') -or
                    ($resource.PSObject.Properties.Name -contains 'external' -and $resource.external)) {
                    throw 'Scenario resources must be new and project-owned'
                }
            }
        }
        $resolved | Set-Content -LiteralPath (Join-Path $destination 'effective-compose.json') -Encoding utf8
        $started = [DateTimeOffset]::UtcNow
        $passed = $false
        try {
            Invoke-ScenarioDocker -DockerArguments ($composeArguments + @('up', '-d', '--wait', '--no-build', 'api', 'worker'))
            Invoke-ScenarioDocker -DockerArguments ($composeArguments + @('run', '--no-deps', '--name', $runner, 'scenario-checks'))
            $passed = $true
        }
        finally {
            $copySucceeded = $false
            try {
                $services = & docker @composeArguments ps -q
                $measurements = @()
                foreach ($container in $services) {
                    $details = & docker inspect $container --format '{{json .}}' | ConvertFrom-Json
                    if ($details.Config.Labels.'com.docker.compose.project' -ne $project) {
                        throw 'Refusing measurement of an unowned container'
                    }
                    $peak = & docker exec $container cat /sys/fs/cgroup/memory.peak
                    $events = & docker exec $container cat /sys/fs/cgroup/memory.events
                    $measurements += [ordered]@{
                        service = $details.Config.Labels.'com.docker.compose.service'
                        image = $details.Image
                        memory_limit_bytes = $details.HostConfig.Memory
                        memory_peak_bytes = [long]$peak
                        memory_events = @($events)
                        oom_killed = $details.State.OOMKilled
                        restart_count = $details.RestartCount
                    }
                    if ($details.State.OOMKilled -or $details.RestartCount -ne 0 -or ($events -match '^oom(_kill|_group_kill)? [1-9]')) { $passed = $false }
                }
                & docker cp ($runner + ':/data/exports/.') $destination
                $copySucceeded = $LASTEXITCODE -eq 0
                & docker logs $runner 2>&1 | Set-Content -LiteralPath (Join-Path $destination 'runner.log') -Encoding utf8
                $receipt = [ordered]@{
                    project = $project; installation_id = $installationId; capacity_profile = $capacityProfile
                    started_utc = $started.ToString('o'); finished_utc = [DateTimeOffset]::UtcNow.ToString('o')
                    passed = $passed -and $copySucceeded; measurements = $measurements
                }
                $receipt | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $destination 'docker-receipt.json') -Encoding utf8
            }
            finally {
                Invoke-ScenarioDocker -DockerArguments ($composeArguments + @('down', '--volumes', '--remove-orphans', '--timeout', '10'))
                $remaining = & docker ps -aq --filter ('label=com.docker.compose.project=' + $project)
                if ($remaining) { throw 'Scenario container cleanup incomplete' }
            }
            if (-not $passed -or -not $copySucceeded) { throw 'Scenario campaign failed; retained local evidence records the failure' }
        }
    }
}
finally {
    [Environment]::SetEnvironmentVariable('SCENARIO_CAPACITY_PROFILE', $previousProfile, 'Process')
    [Environment]::SetEnvironmentVariable('SCENARIO_INSTALLATION_ID', $previousIdentity, 'Process')
}
Write-Host ('Isolated scenario evidence: ' + $campaignRoot)
