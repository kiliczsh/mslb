# Run alongside Docker Desktop startup; keep this process running while accepting work.
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($env:OS -ne "Windows_NT") {
    throw "Windows host memory sampling requires Windows PowerShell and Docker Desktop."
}

$hostBridgeStart = [System.Diagnostics.ProcessStartInfo]::new()
$hostBridgeStart.FileName = "docker"
$hostBridgeStart.Arguments = "compose --profile tools run --rm --no-deps -T host-memory-writer"
$hostBridgeStart.WorkingDirectory = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$hostBridgeStart.UseShellExecute = $false
$hostBridgeStart.CreateNoWindow = $true
$hostBridgeStart.RedirectStandardInput = $true
$hostBridgeProcess = [System.Diagnostics.Process]::new()
$hostBridgeProcess.StartInfo = $hostBridgeStart
$hostBridgeStarted = $false

try {
    if (-not $hostBridgeProcess.Start()) {
        throw "The isolated host memory writer could not start."
    }
    $hostBridgeStarted = $true
    while (-not $hostBridgeProcess.HasExited) {
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
    throw "The isolated host memory writer stopped with exit code $($hostBridgeProcess.ExitCode)."
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
