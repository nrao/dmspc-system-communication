#!/usr/bin/env bash

set -euo pipefail
KAFKA_PROFILES="--profile kafka"
# KAFKA_SERVICES="zookeeper broker kafka-ui ngrok gbt seaweedfs dsoc ngrok-writer vlba"
# KAFKA_SERVICES="zookeeper kafka-broker kafka-ui kafka-init gbt seaweedfs dsoc vlba etr_daemon"

# the order of these services matter!! learned the hard way..
KAFKA_SERVICES="zookeeper kafka-broker kafka-init kafka-ui seaweedfs dsoc-volume-init"
SIM_SERVICES="etr_daemon gbt vlba dsoc"

COMMAND="$1"

case "$COMMAND" in

start)
    echo "Starting development environment..."
    docker compose up -d
    ;;

rebuild)
    echo "Rebuilding development environment..."
    # Take down kafka + sim containers
    docker compose stop $KAFKA_SERVICES
    docker compose rm -f $KAFKA_SERVICES
    docker compose stop $KAFKA_SERVICES
    docker compose rm -f $KAFKA_SERVICES
    # Take down the rest of the containers
    docker compose down
    # --no-cache ensures code changes are baked in cleanly
    docker compose build --no-cache
    # --force-recreate guarantees .env variable updates  and config updates are pushed into the container upon rebuild
    docker compose up -d --force-recreate
    # same with kafka profiles:
    docker compose $KAFKA_PROFILES up -d --force-recreate
    ;;

stop)
    echo "Stopping development environment..."
    docker compose down
    ;;

shell)
    docker compose exec ngradar_website bash
    ;;

logs)
    docker compose logs -f ngradar_website
    ;;

attach)
    docker attach ngradar_website_service
    ;;

load-staging-data)
    docker compose run --rm staging_loader
    ;;


kafka-up)
    echo "Starting Kafka infrastructure and storage..."
    docker compose $KAFKA_PROFILES up -d $KAFKA_SERVICES
    ;;

sims-up)
    echo "Starting simulator services..."
    docker compose $KAFKA_PROFILES up -d $SIM_SERVICES
    ;;

system-up)
    echo "Starting Kafka infrastructure and storage..."
    "$0" kafka-up
    echo "Starting simulator services..."
    "$0" sims-up
    ;;


kafka-down)
    echo "Stopping kafka infrastructure and storage..."
    docker compose stop $KAFKA_SERVICES
    docker compose rm -f $KAFKA_SERVICES
    ;;

sims-down)
    echo "Stopping simulator services..."
    docker compose stop $SIM_SERVICES
    docker compose rm -f $SIM_SERVICES
    ;;

system-down)
    echo "Stopping kafka infrastructure and storage..."
    docker compose stop $KAFKA_SERVICES
    docker compose rm -f $KAFKA_SERVICES
    echo "Stopping simulator services..."
    docker compose stop $SIM_SERVICES
    docker compose rm -f $SIM_SERVICES
    ;;

soft-reset)
    ./control.sh system-down
    ./control.sh stop

    docker volume ls -q \
        | grep -v '^dmspc-system-communication_postgres_data$' \
        | xargs -r docker volume rm

    docker compose build
    docker compose up -d --force-recreate

    ./control.sh system-up
    ;;

testcov)
    echo "Calculating unit test coverage..."
    pytest --cov=ngRadar_Website --cov-report=term-missing
    ;;

hard-reset)

    read -p "This will DELETE your local database and containers. Continue? (y/N): " ANSWER

    if [[ "$ANSWER" != "y" && "$ANSWER" != "Y" ]]; then
        exit 0
    fi

    docker compose down -v --remove-orphans

    docker system prune -f

    docker compose build --no-cache && docker compose up -d
    ;;

*)

    echo "Usage:"
    echo
    echo "./control.sh start"
    echo "./control.sh rebuild"
    echo "./control.sh kafka-up"
    echo "./control.sh kafka-down"
    echo "./control.sh stop"
    echo "./control.sh shell"
    echo "./control.sh logs"
    echo "./control.sh attach"
    echo "./control.sh load-staging-data"
    echo "./control.sh hard-reset"
    echo "./control.sh testcov"
    echo "./control.sh sims-up"
    echo "./control.sh sims-down"
    echo "./control.sh system-up"
    echo "./control.sh system-down"
    exit 1
    ;;

esac