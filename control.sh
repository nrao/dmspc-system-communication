#!/usr/bin/env bash

set -euo pipefail
KAFKA_PROFILES="--profile kafka --profile ngrok"
# "worker" is consumer.py, meaning kafka-up and kafka-down will automatically start and stop the consumer.py worker as well. 
# If you want to run the consumer.py worker separately, you can ommit "worker" from the KAFKA_SERVICES variable and run it separately with "docker compose run --rm worker"
# And bring it down with "docker compose stop worker" and "docker compose rm -f worker", but adding it here does both automatically.
KAFKA_SERVICES="zookeeper broker kafka-ui ngrok gbt seaweedfs dsoc ngrok-writer"

COMMAND="$1"

case "$COMMAND" in

start)
    echo "Starting development environment..."
    docker compose up -d
    ;;

rebuild)
    echo "Rebuilding development environment..."
    docker compose build --no-cache && docker compose up -d
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