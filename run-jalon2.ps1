param([switch]$Build, [switch]$SkipBatch)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
function Invoke-LoggedDocker {
    param([string[]]$DockerArgs, [string]$LogName)
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        & docker @DockerArgs 2>&1 | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { $_.ToString() }
        } | Tee-Object -FilePath "results/$LogName" -ErrorAction Stop
        $dockerCode = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previous }
    if ($dockerCode -ne 0) { throw "Docker failed: $LogName (exit $dockerCode)" }
}
if (-not $SkipBatch) {
    if ($Build) { & ./run.ps1 -Build } else { & ./run.ps1 }
} else {
    if (-not (Test-Path '.env')) { throw 'Run ./run.ps1 -Build once before -SkipBatch.' }
    docker compose up -d --wait --wait-timeout 240
    if ($LASTEXITCODE -ne 0) { throw 'Cluster startup failed' }
}
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/demo_streaming.py') -LogName 'streaming-demo.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/part6_graphframes.py') -LogName 'graphframes.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','sh','tools/run_graphx.sh') -LogName 'graphx.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/validate_graphs.py') -LogName 'graph-validation.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/test_window_contracts.py') -LogName 'window-tests.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/collect_jalon2.py') -LogName 'collect-jalon2.log'
Write-Host 'Jalon 2 termine. Voir results/jalon2/latest.json et results/graph_validation.json.'
