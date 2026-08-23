set shell := ["bash", "-euo", "pipefail", "-c"]

# Run unit tests first, then the complete container-backed integration matrix.
test: test-unit test-integration

# Run the fast suite; integration tests skip when their service URL is absent.
test-unit:
    go test -count=1 ./...

# Run every package across the PostgreSQL 16-18, Redis 7.2/8.10, and RabbitMQ 4.2.9/4.3.5 Cartesian matrix.
test-integration:
    #!/usr/bin/env bash
    set -euo pipefail

    coverage_dir="${REACT_COVERAGE_DIR:-.coverage}"
    mkdir -p "$coverage_dir"
    race_flag=()
    if [[ "${REACT_RACE:-0}" == "1" ]]; then
        race_flag=(-race)
    fi

    containers=()
    cleanup() {
        for container in "${containers[@]}"; do
            docker rm --force "$container" >/dev/null 2>&1 || true
        done
        containers=()
    }

    wait_for_postgres() {
        local container="$1"
        local attempt
        for ((attempt = 1; attempt <= 60; attempt++)); do
            if docker exec "$container" pg_isready -U react -d react >/dev/null 2>&1; then
                return 0
            fi
            if ! docker container inspect "$container" >/dev/null 2>&1; then
                printf 'Container %s stopped before becoming ready\n' "$container" >&2
                docker logs "$container" >&2 || true
                return 1
            fi
            sleep 1
        done
        printf 'Timed out waiting for PostgreSQL container %s\n' "$container" >&2
        docker logs "$container" >&2 || true
        return 1
    }

    wait_for_redis() {
        local container="$1"
        local attempt
        for ((attempt = 1; attempt <= 60; attempt++)); do
            if docker exec "$container" redis-cli ping 2>/dev/null | grep -q '^PONG$'; then
                return 0
            fi
            if ! docker container inspect "$container" >/dev/null 2>&1; then
                printf 'Container %s stopped before becoming ready\n' "$container" >&2
                docker logs "$container" >&2 || true
                return 1
            fi
            sleep 1
        done
        printf 'Timed out waiting for Redis container %s\n' "$container" >&2
        docker logs "$container" >&2 || true
        return 1
    }

    wait_for_rabbitmq() {
        local container="$1"
        local attempt
        for ((attempt = 1; attempt <= 90; attempt++)); do
            # Reading the server log avoids launching a second Erlang VM on
            # every retry, which is important on PID-constrained CI runners.
            if docker logs "$container" 2>&1 | grep -q 'Server startup complete'; then
                return 0
            fi
            if ! docker container inspect "$container" >/dev/null 2>&1; then
                printf 'Container %s stopped before becoming ready\n' "$container" >&2
                docker logs "$container" >&2 || true
                return 1
            fi
            sleep 1
        done
        printf 'Timed out waiting for RabbitMQ container %s\n' "$container" >&2
        docker logs "$container" >&2 || true
        return 1
    }

    published_port() {
        local mapping
        mapping="$(docker port "$1" "$2/tcp")"
        printf '%s\n' "${mapping##*:}"
    }

    trap cleanup EXIT
    postgres_matrix=(
        "16|postgres:16"
        "17|postgres:17"
        "18|postgres:18"
    )
    redis_matrix=(
        "7.2|${REACT_REDIS_72_IMAGE:-redis:7.2-alpine}"
        "8.10|${REACT_REDIS_810_IMAGE:-redis:8.10-alpine}"
    )
    rabbitmq_matrix=(
        "4.2.9|${REACT_RABBITMQ_429_IMAGE:-rabbitmq:4.2.9-alpine}"
        "4.3.5|${REACT_RABBITMQ_435_IMAGE:-rabbitmq:4.3.5-alpine}"
    )

    for postgres_target in "${postgres_matrix[@]}"; do
        IFS='|' read -r postgres_major postgres_image <<<"$postgres_target"
        for redis_target in "${redis_matrix[@]}"; do
            IFS='|' read -r redis_version redis_image <<<"$redis_target"
            for rabbitmq_target in "${rabbitmq_matrix[@]}"; do
                IFS='|' read -r rabbitmq_version rabbitmq_image <<<"$rabbitmq_target"
                printf '\nTesting React packages with PostgreSQL %s, Redis %s, and RabbitMQ %s\n' \
                    "$postgres_major" "$redis_version" "$rabbitmq_version"

                cleanup
                suffix="pg-${postgres_major}-redis-${redis_version}-rabbitmq-${rabbitmq_version}-$$"
                postgres_container="react-postgres-${suffix}"
                redis_container="react-redis-${suffix}"
                rabbitmq_container="react-rabbitmq-${suffix}"
                containers+=("$postgres_container" "$redis_container" "$rabbitmq_container")

                docker run --detach \
                    --name "$postgres_container" \
                    --env POSTGRES_DB=react \
                    --env POSTGRES_USER=react \
                    --env POSTGRES_PASSWORD=react \
                    --publish 127.0.0.1::5432 \
                    "$postgres_image" >/dev/null

                docker run --detach \
                    --name "$redis_container" \
                    --publish 127.0.0.1::6379 \
                    "$redis_image" \
                    redis-server --appendonly yes --appendfsync everysec >/dev/null

                docker run --detach \
                    --name "$rabbitmq_container" \
                    --env RABBITMQ_DEFAULT_USER=react \
                    --env RABBITMQ_DEFAULT_PASS=react \
                    --env 'RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=+S 2:2' \
                    --publish 127.0.0.1::5672 \
                    "$rabbitmq_image" >/dev/null

                wait_for_postgres "$postgres_container"
                wait_for_redis "$redis_container"
                wait_for_rabbitmq "$rabbitmq_container"
                postgres_port="$(published_port "$postgres_container" 5432)"
                redis_port="$(published_port "$redis_container" 6379)"
                rabbitmq_port="$(published_port "$rabbitmq_container" 5672)"
                postgres_url="postgres://react:react@127.0.0.1:${postgres_port}/react?sslmode=disable"
                redis_url="redis://127.0.0.1:${redis_port}/0"
                rabbitmq_url="amqp://react:react@127.0.0.1:${rabbitmq_port}/"
                coverage_profile="${coverage_dir}/postgres-${postgres_major}-redis-${redis_version}-rabbitmq-${rabbitmq_version}.coverprofile"

                REACT_REQUIRE_INTEGRATION=1 \
                POSTGRES_TEST_URL="$postgres_url" \
                OUTBOX_POSTGRES_TEST_URL="$postgres_url" \
                REDIS_TEST_URL="$redis_url" \
                OUTBOX_REDIS_TEST_URL="$redis_url" \
                OUTBOX_REDIS_ALLOW_SCRIPT_FLUSH=1 \
                RMQ_TEST_URL="$rabbitmq_url" \
                    go test -count=1 "${race_flag[@]}" -covermode=atomic -coverprofile="$coverage_profile" ./...

                printf 'Coverage profile: %s\n' "$coverage_profile"
                go tool cover -func="$coverage_profile" | tail -n 1
                cleanup
            done
        done
    done
