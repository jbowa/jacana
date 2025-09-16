# List available commands
default:
    just --list

# Migrate up
up:
    #!/usr/bin/env bash
    set -euo pipefail
    export GOOSE_DRIVER=clickhouse
    export GOOSE_DBSTRING="tcp://default:P@ssword@localhost:9000/default"
    goose -dir migrations up

# Migrate down
down:
    #!/usr/bin/env bash
    set -euo pipefail
    export GOOSE_DRIVER=clickhouse
    export GOOSE_DBSTRING="tcp://default:P@ssword@localhost:9000/default"
    goose -dir migrations down

# Show migration status
status:
    #!/usr/bin/env bash
    set -euo pipefail
    export GOOSE_DRIVER=clickhouse
    export GOOSE_DBSTRING="tcp://default:P@ssword@localhost:9000/default"
    goose -dir migrations status

# Create new migration
create NAME:
    #!/usr/bin/env bash
    set -euo pipefail
    export GOOSE_DRIVER=clickhouse
    export GOOSE_DBSTRING="tcp://default:P@ssword@localhost:9000/default"
    goose -dir migrations create {{NAME}} sql