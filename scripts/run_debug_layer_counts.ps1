# Run debug_layer_counts.py via Docker (avoids local Spark/JVM compatibility issues)
# Usage: .\scripts\run_debug_layer_counts.ps1
#        .\scripts\run_debug_layer_counts.ps1 --category Gift_Cards

Set-Location $PSScriptRoot\..
docker compose -f docker/docker-compose.yml run --rm dashboard python3 scripts/debug_layer_counts.py $args
