#!/usr/bin/env bash

set -euo pipefail

set -a
source .env
set +a

# For local dev, should only need to run these commands to have everything you need:
#  ./control.sh start      --> Will start website, db, routing services
#  ./control.sh system-up  --> Will start kafka_services, sim_services, metric_services
#  ./control.sh system-down
#  ./control.sh stop
#
# For DO Droplets only, use these:
#    To control all droplets from a remote device, use the following commands:
#      ./control.sh droplets-up
#      ./control.sh droplets-down
#    If you are currently on a droplet, use one of the following commands:
#      ./control.sh dsoc-up
#      ./control.sh dsoc-down
#      ./control.sh gbt-up
#      ./control.sh gbt-down
#      ./control.sh vlba-up VLBA_#_SERVICES
#      ./control.sh vlba-down VLBA_#_SERVICES
#      ./control.sh portainer-up
#      ./control.sh agent-up



START="traefik_http ngradar_website postgres"

DSOC_DROPLET="root@${DSOC_DROPLET_IP}"
VLBA_1_DROPLET="root@${VLBA_1_DROPLET_IP}"
VLBA_2_DROPLET="root@${VLBA_2_DROPLET_IP}"
VLBA_3_DROPLET="root@${VLBA_3_DROPLET_IP}"
GBT_DROPLET="root@${GBT_DROPLET_IP}"

REMOTE_DIR="/root/${REMOTE_REPO}"

KAFKA_PROFILES="--profile kafka"

# the order of these services matter!! learned the hard way..
KAFKA_SERVICES="kafka-node-1 kafka-node-2 kafka-node-3 kafka-ui seaweedfs dsoc-volume-init db_consumer"
SIM_SERVICES="etr_daemon gbt vlba-sc vlba-hn vlba-nl vlba-fd vlba-la vlba-pt vlba-kp vlba-ov vlba-br vlba-mk dsoc progress_tracker"
METRIC_SERVICES="portainer prometheus grafana otel-collector postgres_exporter kafka-exporter k6 tempo"

PORTAINER_SERVICE="portainer"
AGENT_SERVICE="portainer_agent"

DSOC_SERVICES="traefik ngradar_website postgres prometheus tempo otel-collector grafana postgres_exporter kafka-init kafka-ui kafka-exporter seaweedfs dsoc-volume-init dsoc etr_daemon progress_tracker db_consumer vlba-kp vlba-ov"
VLBA_1_SERVICES="vlba-sc vlba-hn"
VLBA_2_SERVICES="vlba-nl vlba-fd"
VLBA_3_SERVICES="vlba-la vlba-pt"
GBT_SERVICES="vlba-br vlba-mk gbt"

COMMAND="$1"

case "$COMMAND" in

start)
    echo "Starting development environment..."
    docker compose up -d $START
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

kafka-up)
    echo "Starting Kafka infrastructure and storage..."
    docker compose $KAFKA_PROFILES up -d $KAFKA_SERVICES
    echo "Initializing Kafka topics..."
    docker compose $KAFKA_PROFILES up kafka-init
    ;;

sims-up)
    echo "Starting simulator services..."
    docker compose $KAFKA_PROFILES up -d $SIM_SERVICES
    docker compose up -d $METRIC_SERVICES
    ;;

system-up)
    echo "Starting Kafka infrastructure and storage..."
    "$0" kafka-up
    echo "Starting website, db, traefik routing..."
    "$0" start
    echo "Starting simulator services..."
    "$0" sims-up
    ;;

system-down)
    echo "Stopping kafka infrastructure and storage..."
    docker compose stop $KAFKA_SERVICES
    docker compose rm -f $KAFKA_SERVICES
    docker compose stop kafka-init
    docker compose rm -f kafka-init
    echo "Stopping simulator services..."
    docker compose stop $SIM_SERVICES
    docker compose rm -f $SIM_SERVICES
    echo "Stopping metric services..."
    docker compose stop $METRIC_SERVICES
    docker compose rm -f $METRIC_SERVICES
    echo "Stopping website, db, routing services..."
    docker compose stop $START
    docker compose rm -f $START
    ;;


rebuild)
    # Rebuild one or more specified containers.

    shift

     if [ "$#" -eq 0 ]; then
        echo "Error: Specify at least one service to rebuild."
        echo
        echo "Usage:"
        echo "  ./control.sh rebuild <service> <service> . . ."
        exit 1
    fi

    echo "Rebuilding services: $*"

    # Rebuild selected services with fresh images.
    docker compose build --no-cache "$@"

    # Recreate only the selected services so image,
    # environment, and Compose config changes are applied.
    docker compose up -d --force-recreate --no-deps "$@"
    ;;


rebuild-all)
    # Will rebuild ALL containers for local dev used in local dev
    ./control.sh system-down

    docker volume ls -q \
        | grep -v 'postgres_data$' \
        | xargs -r docker volume rm || true

    # --no-cache ensures code changes are baked in cleanly
    docker compose build $KAFKA_SERVICES --no-cache
    docker compose build kafka-init --no-cache
    docker compose build $SIM_SERVICES --no-cache
    docker compose build $START --no-cache
    docker compose build $METRIC_SERVICES --no-cache

    # --force-recreate guarantees .env variable updates  and config updates are pushed into the container upon rebuild
    docker compose up -d --force-recreate $KAFKA_SERVICES
    docker compose up -d kafka-init --force-recreate
    docker compose up -d --force-recreate $START
    docker compose up -d --force-recreate $SIM_SERVICES
    docker compose up -d --force-recreate $METRIC_SERVICES
    ;;


refresh)
    # Recreate containers so updated .env values are injected.
    # Images are reused; no rebuild is performed.
    docker compose up -d --force-recreate $KAFKA_SERVICES
    docker compose up -d kafka-init --force-recreate
    docker compose up -d --force-recreate $START
    docker compose up -d --force-recreate $SIM_SERVICES
    docker compose up -d --force-recreate $METRIC_SERVICES

    ;;

testcov)
    echo "Calculating unit test coverage..."
    pytest --cov=ngRadar_Website --cov-report=term-missing
    ;;

smoketest)
    docker compose --profile loadtest run --rm k6 \
        run --out experimental-prometheus-rw /scripts/smoke.js
    ;;

journeytest)
    docker compose --profile loadtest run --rm k6 \
        run --out experimental-prometheus-rw /scripts/journey.js
    ;;

loadtest)
    timestamp=$(date +%Y%m%d-%H%M%S)

    docker compose --profile loadtest run --rm \
        -e RUN_ID="load-${timestamp}" \
        k6 run \
        --out experimental-prometheus-rw \
        --out "json=/results/breakpoint_${timestamp}.json" \
        /scripts/load.js
        ;;         # add another output to save raw results to a file for later analysis (needed if doing a breakpoint test) --out "json=/results/breakpoint_${timestamp}.json"

hard-reset)
    read -p "This will DELETE your local database and containers. Continue? (y/N): " ANSWER

    if [[ "$ANSWER" != "y" && "$ANSWER" != "Y" ]]; then
        exit 0
    fi

    docker compose down -v --remove-orphans

    docker system prune -f

     # --no-cache ensures code changes are baked in cleanly
    docker compose build $KAFKA_SERVICES --no-cache
    docker compose build kafka-init --no-cache
    docker compose build $SIM_SERVICES --no-cache
    docker compose build $START --no-cache
    docker compose build $METRIC_SERVICES --no-cache

    # --force-recreate guarantees .env variable updates  and config updates are pushed into the container upon rebuild
    docker compose up -d --force-recreate $KAFKA_SERVICES
    docker compose up -d kafka-init --force-recreate
    docker compose up -d --force-recreate $START
    docker compose up -d --force-recreate $SIM_SERVICES
    docker compose up -d --force-recreate $METRIC_SERVICES
    ;;

portainer-up)
    docker compose up -d $PORTAINER_SERVICE
    ;;

agent-up)
    docker compose up -d $AGENT_SERVICE
    ;;

gbt-up)
    docker compose up -d $GBT_SERVICES
    ;;

gbt-down)
    docker compose stop $GBT_SERVICES
    docker compose rm -f $GBT_SERVICES
    ;;

vlba-up)
    # Get the services that are passed as the argument to vlba-up
    SERVICES="${!2}"
    docker compose up -d $SERVICES
    ;;

vlba-down)
    # Get the services that are passed as the argument to vlba-down
    SERVICES="${!2}"
    docker compose stop $SERVICES
    docker compose rm -f $SERVICES
    ;;

dsoc-up)
    docker compose up -d $DSOC_SERVICES
    ;;

dsoc-down)
    docker compose stop $DSOC_SERVICES
    docker compose rm -f $DSOC_SERVICES
    ;;

droplets-up)
    read -p "Are you sure you want to ssh into the DIGITALOCEAN DROPLETS? This will START all of the DIGITALOCEAN DROPLETS from your LOCAL TERMINAL. Continue? (y/N): " ANSWER

    if [[ "$ANSWER" != "y" && "$ANSWER" != "Y" ]]; then
        exit 0
    fi

    echo "Starting DSOC Droplet"
    ssh "$DSOC_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && git pull && ./control.sh dsoc-up"

    echo "Starting VLBA 1 Droplet"
    ssh "$VLBA_1_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && git pull && ./control.sh vlba-up VLBA_1_SERVICES"

    echo "Starting VLBA 2 Droplet"
    ssh "$VLBA_2_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && git pull && ./control.sh vlba-up VLBA_2_SERVICES"

    echo "Starting VLBA 3 Droplet"
    ssh "$VLBA_3_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && git pull && ./control.sh vlba-up VLBA_3_SERVICES"

    echo "Starting GBT Droplet"
    ssh "$GBT_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && git pull && ./control.sh gbt-up"
    ;;

droplets-down)
    read -p "Are you sure you want to ssh into the DIGITALOCEAN DROPLETS? This will STOP all of the DIGITALOCEAN DROPLETS from your LOCAL TERMINAL. Continue? (y/N): " ANSWER

    if [[ "$ANSWER" != "y" && "$ANSWER" != "Y" ]]; then
        exit 0
    fi

    echo "Stopping GBT Droplet"
    ssh "$GBT_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && ./control.sh gbt-down"

    echo "Stopping VLBA 1 Droplet"
    ssh "$VLBA_1_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && ./control.sh vlba-down VLBA_1_SERVICES"

    echo "Stopping VLBA 2 Droplet"
    ssh "$VLBA_2_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && ./control.sh vlba-down VLBA_2_SERVICES"

    echo "Stopping VLBA 3 Droplet"
    ssh "$VLBA_3_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && ./control.sh vlba-down VLBA_3_SERVICES"

    echo "Stopping DSOC Droplet"
    ssh "$DSOC_DROPLET" \
        "cd $REMOTE_DIR && git checkout dev && ./control.sh dsoc-down"
    ;;
*)
    echo
    echo "HELP with ./control.sh usage:"
    echo
    echo "If developing locally, use the following commands to start/stop your environment:"
    echo "./control.sh system-up"
    echo "./control.sh system-down"
    echo
    echo "To make migrations and create superusers, use the shell:"
    echo "./control.sh shell"
    echo
    echo "Utility commands to rebuild local dev environment:"
    echo "./control.sh rebuild <service> <service> ..."
    echo "./control.sh rebuild-all"
    echo "./control.sh hard-reset"
    echo
    echo "To run test coverage on this branch, run:"
    echo "./control.sh testcov"
    echo
    echo "To control all droplets from a remote device, use the following commands:"
    echo "./control.sh droplets-up"
    echo "./control.sh droplets-down"
    echo
    echo "If you are currently on a droplet, use one of the following commands:"
    echo "./control.sh dsoc-up"
    echo "./control.sh dsoc-down"
    echo "./control.sh gbt-up"
    echo "./control.sh gbt-down"
    echo "./control.sh vlba-up VLBA_#_SERVICES"
    echo "./control.sh vlba-down VLBA_#_SERVICES"
    echo "./control.sh portainer-up"
    echo "./control.sh agent-up"
    exit 1
    ;;

esac