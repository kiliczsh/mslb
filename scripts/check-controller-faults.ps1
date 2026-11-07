# Project-scoped real interruption checks; never stop the Docker daemon or unrelated resources.
[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
Push-Location (Join-Path $PSScriptRoot '..')
New-Item -ItemType Directory -Force -Path '.cache/verification' | Out-Null
$headers = @{}
$evidence = [System.Collections.Generic.List[object]]::new()
$csvPath = Join-Path (Get-Location).Path ('.cache/verification/fault-' + [guid]::NewGuid().ToString('N') + '.csv')
function Invoke-FaultDocker {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw 'Project fault Docker operation failed' }
}
function Get-FaultView {
    param([string]$RunId)
    Invoke-RestMethod -Uri ("http://127.0.0.1:8000/api/v1/docker/simulations/$RunId") -Headers $headers -TimeoutSec 5
}
function Wait-FaultAdmission {
    $until = [DateTime]::UtcNow.AddSeconds(20)
    do {
        $ready = Invoke-WebRequest -Uri 'http://127.0.0.1:8000/api/v1/health/ready' -Headers $headers -SkipHttpErrorCheck -TimeoutSec 5
        if ($ready.StatusCode -notin @(200, 503)) { throw 'Resource readiness request was refused' }
        $body = $ready.Content | ConvertFrom-Json
        if ($null -ne $body.resource_admission -and $body.resource_admission.admit) { return }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $until)
    throw 'Fresh resource admission did not recover'
}
function New-FaultRun {
    Wait-FaultAdmission
    $dataset = Invoke-RestMethod -Uri 'http://127.0.0.1:8000/api/v1/datasets' -Method Post -Headers $headers -Form @{ file = Get-Item -LiteralPath $csvPath } -TimeoutSec 10
    $configuration = @{ mode='docker'; dataset_id=$dataset.id; initial_replicas=3; global_inflight=2; backend_inflight=1; starts_per_backend_second=1; pending_timeout_seconds=120; duration_seconds=120 }
    $requestHeaders = $headers.Clone()
    $requestHeaders['Idempotency-Key'] = 'fault-' + [guid]::NewGuid().ToString('N')
    $accepted = Invoke-RestMethod -Uri 'http://127.0.0.1:8000/api/v1/docker/simulations' -Method Post -Headers $requestHeaders -ContentType 'application/json' -Body ($configuration | ConvertTo-Json) -TimeoutSec 10
    $until = [DateTime]::UtcNow.AddSeconds(15)
    do {
        $view = Get-FaultView -RunId $accepted.run_id
        if ($view.run.status -eq 'running' -and $null -ne $view.progress -and @($view.resources | Where-Object { $_.kind -eq 'dynamic' -and $_.state -eq 'started' }).Count -eq 1) { return $accepted.run_id }
        if ($view.run.status -in @('failed','cancelled','completed')) { throw 'Fault setup did not retain actual running owned work' }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $until)
    throw 'Actual owned physical work did not start within its bound'
}
function Wait-FaultCleanup {
    param([string]$RunId, [string]$Case)
    $until = [DateTime]::UtcNow.AddSeconds(60)
    do {
        $view = Get-FaultView -RunId $RunId
        if ($view.run.status -eq 'completed') { throw 'Interrupted physical work published success' }
        if ($view.run.status -eq 'failed' -and -not $view.cleanup_pending) {
            if ($view.run.attempts -ne 1 -or @($view.artifacts).Count -ne 0 -or $null -ne $view.summary -or @($view.resources | Where-Object { $_.state -notin @('removed','released','abandoned') }).Count -ne 0) { throw 'Interrupted physical ownership/accounting differs' }
            $evidence.Add([ordered]@{ case=$Case; run_id=$RunId; status=$view.run.status; error_code=$view.run.error_code; attempts=$view.run.attempts; cleanup_pending=$view.cleanup_pending; resources=@($view.resources | Select-Object id,name,kind,state) })
            return
        }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $until)
    throw 'Interrupted physical cleanup exceeded its bounded recovery window'
}
try {
    $resolved = & docker compose --profile tools config --format json
    if ($LASTEXITCODE -ne 0) { throw 'Fault configuration resolution failed' }
    $config = $resolved | ConvertFrom-Json
    if ($config.name -ne 'mslb') { throw 'Refusing an unrelated project' }
    if ($config.services.api.environment.AUTH_ENABLED -eq 'true') { $headers.Authorization = 'Bearer ' + $config.services.api.environment.AUTH_TOKEN }
    $rows = [System.Collections.Generic.List[string]]::new()
    $rows.Add('t,request_id,work_units,mem_mb')
    for ($index=0; $index -lt 48; $index++) { $rows.Add("0,fault-$index,1000,1") }
    [System.IO.File]::WriteAllText($csvPath, (($rows -join "`n") + "`n"), [System.Text.UTF8Encoding]::new($false))

    $run = New-FaultRun
    Invoke-FaultDocker -Arguments @('compose','kill','--signal','SIGKILL','container-controller')
    Invoke-FaultDocker -Arguments @('compose','up','--no-build','-d','--wait','container-controller')
    Wait-FaultCleanup -RunId $run -Case 'controller process loss without replay'

    $run = New-FaultRun
    try {
        Invoke-FaultDocker -Arguments @('compose','stop','backend-a')
        Wait-FaultCleanup -RunId $run -Case 'stopped static backend identity recovery'
    } finally { Invoke-FaultDocker -Arguments @('compose','up','--no-build','-d','--wait','backend-a') }
    Wait-FaultAdmission

    $run = New-FaultRun
    try {
        Invoke-FaultDocker -Arguments @('compose','stop','docker-proxy')
        $unavailableUntil = [DateTime]::UtcNow.AddSeconds(10)
        $observedUnavailable = $false
        do {
            $state = (Invoke-WebRequest -Uri 'http://127.0.0.1:8000/api/v1/health/ready' -Headers $headers -SkipHttpErrorCheck -TimeoutSec 5).Content | ConvertFrom-Json
            if ($null -ne $state.resource_admission -and $state.resource_admission.stop_running) { $observedUnavailable = $true; break }
            Start-Sleep -Milliseconds 200
        } while ([DateTime]::UtcNow -lt $unavailableUntil)
        if (-not $observedUnavailable) { throw 'The actual restricted-proxy outage was not observed' }

    } finally { Invoke-FaultDocker -Arguments @('compose','up','--no-build','-d','--wait','docker-proxy') }
    Wait-FaultCleanup -RunId $run -Case 'restricted control proxy outage'
    Wait-FaultAdmission
    $ids = @(& docker compose ps --all -q)
    if ($LASTEXITCODE -ne 0 -or $ids.Count -eq 0) { throw 'Project inspection failed' }
    $oom = @(& docker inspect --format '{{.State.OOMKilled}}' @ids)
    if ($LASTEXITCODE -ne 0 -or @($oom | Where-Object { $_ -ne 'false' }).Count -ne 0) { throw 'A project container reported OOM' }
    $evidence | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath '.cache/verification/controller-faults.json' -Encoding utf8
    Write-Host 'Actual controller interruption, stopped-backend recovery and restricted-proxy outage passed; no project OOM kills.'
} finally { Pop-Location }
