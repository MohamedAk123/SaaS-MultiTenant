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
$otherEnv = Join-Path $PSScriptRoot '../jalon-1/.env'
if ((-not (Test-Path -LiteralPath '.env')) -and (Test-Path -LiteralPath $otherEnv)) {
    Copy-Item -LiteralPath $otherEnv -Destination '.env'
}
if (-not (Test-Path -LiteralPath '.env')) {
    $password = [guid]::NewGuid().ToString('N')
    $token = [guid]::NewGuid().ToString('N')
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot '.env'), "MINIO_ROOT_USER=saasadmin`nMINIO_ROOT_PASSWORD=$password`nJUPYTER_TOKEN=$token`n")
}
New-Item -ItemType Directory -Force -Path data,results,evidence | Out-Null
if ($Build) {
    docker compose build
    if ($LASTEXITCODE -ne 0) { throw 'Docker build failed' }
}
docker compose up -d --wait --wait-timeout 240
if ($LASTEXITCODE -ne 0) { throw 'Cluster startup failed' }
if (-not $SkipBatch) {
    Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/producer.py','--mode','file','--n','50000') -LogName 'prepare-dataset.log'
    Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/bootstrap.py') -LogName 'bootstrap.log'
} elseif (-not (Test-Path 'data/orders_history.jsonl')) {
    throw 'Dataset not prepared. Run without -SkipBatch first.'
}
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/demo_streaming.py') -LogName 'streaming-demo.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/part6_graphframes.py') -LogName 'graphframes.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','sh','tools/run_graphx.sh') -LogName 'graphx.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/validate_graphs.py') -LogName 'graph-validation.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/test_window_contracts.py') -LogName 'window-tests.log'
Invoke-LoggedDocker -DockerArgs @('compose','exec','-T','jupyter','python3','src/collect_jalon2.py') -LogName 'collect-jalon2.log'
Write-Host 'Jalon 2 termine. Voir results/jalon2/latest.json et results/graph_validation.json.'
