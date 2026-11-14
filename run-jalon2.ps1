param([switch]$Build, [switch]$SkipBatch)
& (Join-Path $PSScriptRoot 'jalon-2/run.ps1') @PSBoundParameters
