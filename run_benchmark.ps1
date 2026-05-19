$projectDir = "tests/TickWriteBenchmark"
$project = Join-Path $projectDir "TickWriteBenchmark.csproj"

Write-Host "Building and running TickWriteBenchmark..." -ForegroundColor Green
dotnet run --project $project -c Release

if ($LASTEXITCODE -ne 0) {
    Write-Host "Benchmark failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "Benchmark completed successfully." -ForegroundColor Green
