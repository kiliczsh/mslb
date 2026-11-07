# Run alongside Docker Desktop startup; keep this process running while accepting work.
[CmdletBinding()]
param([string]$StopFile = '', [string]$ContainerName = '')

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($env:OS -ne "Windows_NT") {
    throw "Windows host memory sampling requires Windows PowerShell and Docker Desktop."
}
if ($ContainerName -and $ContainerName -notmatch '^mslb-host-memory-[a-f0-9]{12}$') {
    throw 'Host memory writer name must be a unique project-owned identifier.'
}

$hostBridgeStart = [System.Diagnostics.ProcessStartInfo]::new()
$hostBridgeStart.FileName = "docker"
$hostBridgeNameArgument = if ($ContainerName) { "--name $ContainerName " } else { '' }
$hostBridgeStart.Arguments = "compose --profile tools run --rm --no-deps -T ${hostBridgeNameArgument}host-memory-writer"
$hostBridgeStart.WorkingDirectory = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$hostBridgeStart.UseShellExecute = $false
$hostBridgeStart.CreateNoWindow = $true
$hostBridgeStart.RedirectStandardInput = $true
$hostBridgeStart.RedirectStandardError = $true
$hostBridgeProcess = [System.Diagnostics.Process]::new()
$hostBridgeProcess.StartInfo = $hostBridgeStart
$hostBridgeStarted = $false
$hostBridgeStopped = $false

try {
    if (-not $hostBridgeProcess.Start()) {
        throw "The isolated host memory writer could not start."
    }
    $hostBridgeStarted = $true
    while (-not $hostBridgeProcess.HasExited) {
        if ($StopFile -and (Test-Path -LiteralPath $StopFile)) {
            $hostBridgeStopped = $true
            break
        }
        $hostOperatingSystem = Get-CimInstance -ClassName Win32_OperatingSystem `
            -Property TotalVisibleMemorySize, FreePhysicalMemory
        $hostTotalBytes = [long]([decimal]$hostOperatingSystem.TotalVisibleMemorySize * 1024)
        $hostAvailableBytes = [long]([decimal]$hostOperatingSystem.FreePhysicalMemory * 1024)
        if ($hostTotalBytes -le 0 -or $hostAvailableBytes -lt 0 -or `
            $hostAvailableBytes -gt $hostTotalBytes) {
            throw "Windows returned inconsistent physical memory counters."
        }
        $hostFrame = [ordered]@{
            schema_version = 1
            measured_at = [DateTime]::UtcNow.ToString("o")
            total_memory_bytes = $hostTotalBytes
            available_memory_bytes = $hostAvailableBytes
        } | ConvertTo-Json -Compress
        if ($hostFrame.Length -gt 511) {
            throw "Windows host memory frame exceeds its bounded transport."
        }
        $hostBridgeProcess.StandardInput.WriteLine($hostFrame)
        $hostBridgeProcess.StandardInput.Flush()
        Start-Sleep -Milliseconds 1000
    }
    if (-not $hostBridgeStopped) {
        $hostBridgeBuffer = [char[]]::new(4096)
        $hostBridgeRead = $hostBridgeProcess.StandardError.Read($hostBridgeBuffer, 0, $hostBridgeBuffer.Length)
        $hostBridgeDiagnostic = [string]::new($hostBridgeBuffer, 0, $hostBridgeRead)
        throw "The isolated host memory writer stopped with exit code $($hostBridgeProcess.ExitCode): $hostBridgeDiagnostic"
    }
}
finally {
    if ($hostBridgeStarted) {
        $hostBridgeProcess.StandardInput.Close()
        if (-not $hostBridgeProcess.WaitForExit(5000)) {
            $hostBridgeProcess.Kill()
        }
    }
    $hostBridgeProcess.Dispose()
}
