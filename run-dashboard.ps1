param([switch]$Build)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot 'jalon-2')
$otherEnv = Join-Path $PSScriptRoot 'jalon-1/.env'
if (-not (Test-Path -LiteralPath '.env')) {
    if (Test-Path -LiteralPath $otherEnv) {
        Copy-Item -LiteralPath $otherEnv -Destination '.env'
    } else {
        $password = [guid]::NewGuid().ToString('N')
        $token = [guid]::NewGuid().ToString('N')
        [IO.File]::WriteAllText((Join-Path (Get-Location).Path '.env'), "MINIO_ROOT_USER=saasadmin`nMINIO_ROOT_PASSWORD=$password`nJUPYTER_TOKEN=$token`n")
    }
}
if (-not (Test-Path -LiteralPath '.env.dashboard')) {
    $secret = [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')
    [IO.File]::WriteAllText((Join-Path (Get-Location).Path '.env.dashboard'), "SAAS_SECRET=$secret`n")
}
$compose = @('compose', '--env-file', '.env', '--env-file', '.env.dashboard', '-f', 'docker-compose.yml', '-f', 'compose.dashboard.yml')
if ($Build) {
    & docker @compose build dashboard spark-master minio
} else {
    & docker @compose build dashboard
}
if ($LASTEXITCODE -ne 0) { throw 'Image build failed' }
& docker @compose up -d --wait --wait-timeout 180
if ($LASTEXITCODE -ne 0) { throw 'Dashboard startup failed' }
Write-Host 'Dashboard: http://localhost:3000'
Write-Host 'Local tenant credentials:'
& docker @compose exec -T dashboard node worker.mjs --credentials
