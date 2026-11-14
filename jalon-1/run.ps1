﻿param([switch]$Build)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$otherEnv = Join-Path $PSScriptRoot '../jalon-2/.env'
if ((-not (Test-Path -LiteralPath '.env')) -and (Test-Path -LiteralPath $otherEnv)) {
    Copy-Item -LiteralPath $otherEnv -Destination '.env'
}
if (-not (Test-Path -LiteralPath '.env')) {
    $localPassword = [guid]::NewGuid().ToString('N')
    $localToken = [guid]::NewGuid().ToString('N')
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot '.env'), "MINIO_ROOT_USER=saasadmin`nMINIO_ROOT_PASSWORD=$localPassword`nJUPYTER_TOKEN=$localToken`n")
}
New-Item -ItemType Directory -Force -Path data,results,evidence | Out-Null
if ($Build) {
    docker compose build
    if ($LASTEXITCODE -ne 0) { throw 'Docker build failed' }
}
docker compose up -d --wait --wait-timeout 240
if ($LASTEXITCODE -ne 0) { throw 'Cluster startup failed' }
docker compose exec -T jupyter python3 src/producer.py --mode file --n 50000
if ($LASTEXITCODE -ne 0) { throw 'Data generation failed' }
foreach ($script in @('bootstrap.py','part1_cluster.py','part2_rdd.py','part3_sql.py','validate_results.py','collect_evidence.py')) {
    # Windows PowerShell 5.1 turns native stderr into ErrorRecord objects.
    # Spark writes normal startup messages there: use Docker's exit code.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        docker compose exec -T jupyter python3 "src/$script" 2>&1 |
            ForEach-Object {
                if ($_ -is [System.Management.Automation.ErrorRecord]) {
                    $_.Exception.Message
                }
                else {
                    $_.ToString()
                }
            } |
            Tee-Object -FilePath "results/$script.log" -ErrorAction Stop
        $dockerExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($dockerExitCode -ne 0) { throw "$script failed (Docker exit code: $dockerExitCode)" }
}
Write-Host 'Jalon 1 validé. Résultats dans results/. Interfaces et explications dans README.md.'
