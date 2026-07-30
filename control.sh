#!/usr/bin/env bash

set -euo pipefail
KAFKA_PROFILES="--profile kafka"

KAFKA_SERVICES="zookeeper broker kafka-ui ngrok gbt seaweedfs dsoc vlba ngrok-writer"

COMMAND="$1"

case "$COMMAND" in

start)
    echo "Starting development environment..."
    docker compose up -d
    ;;

rebuild)
    echo "Rebuilding development environment..."
    # Take down kafka containers
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
    echo "Starting kafka broker, zookeeper, kafka-ui, ngrok, seaweedfs, workers..."
    docker compose $KAFKA_PROFILES up -d
    ;;


kafka-down)
    echo "Stopping kafka broker, zookeeper, kafka-ui, ngrok, seaweedfs, workers..."
    docker compose stop $KAFKA_SERVICES
    docker compose rm -f $KAFKA_SERVICES
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
    exit 1
    ;;

esac