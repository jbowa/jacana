# List available commands
default:
    just --list

# Apply all pending migrations
migrate-up:
    #!/usr/bin/env bash
    set -euo pipefail
    export GOOSE_DRIVER=clickhouse
    export GOOSE_DBSTRING="tcp://default:password@localhost:9000/default"
    goose -dir migrations up

# Rollback the last migration
migrate-down:
    #!/usr/bin/env bash
    set -euo pipefail
    export GOOSE_DRIVER=clickhouse
    export GOOSE_DBSTRING="tcp://default:password@localhost:9000/default"
    goose -dir migrations down

# Show migration status
migrate-status:
    #!/usr/bin/env bash
    set -euo pipefail
    export GOOSE_DRIVER=clickhouse
    export GOOSE_DBSTRING="tcp://default:password@localhost:9000/default"
    goose -dir migrations status

# Create new migration file
migrate-create NAME:
    #!/usr/bin/env bash
    set -euo pipefail
    export GOOSE_DRIVER=clickhouse
    export GOOSE_DBSTRING="tcp://default:password@localhost:9000/default"
    goose -dir migrations create {{NAME}} sql
