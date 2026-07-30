# dmspc-system-communication
Code for prototyping system communication for the ngRadar project. This code is written by the Data Management & Software Prototyping Cohort (DMSPC).


# How to run locally:
 File | Purpose |
| `Dockerfile` | Builds the app image: installs Python deps, collects Django static files, runs the dev server |
| `Docker-compose.yml file.` | Orchestrates the app container + a Postgres container on a shared network |
| `load_staging_data.Dockerfile` | A separate, one-off image used only to dump/load staging data from our demo db to the local DB 
|                                | for local development only|
| `control.sh` | A thin wrapper script so the team doesn't need to memorize `docker compose` invocations |
| `.env` | Environment variables consumed by `/settings.py` (our .env file is never committed to Github - please ask team for the .env file for local dev) |

# Start the Virtual Environment
# source .venv/bin/activate

# create a new superuser
# ./control.sh shell
# python3 manage.py createsuperuser
# exit

## For your convenience, use the below `control.sh` wrapper controls:

These `control.sh` wrap common operations.
Run these commands in your terminal to accomplish any of the following:
```
./control.sh start          # Does a `docker compose up -d`
                            # ^ if you get an error, make sure your Docker Desktop is open
./control.sh rebuild        # Does a `docker compose up -d --build` so it starts + rebuilds image
./control.sh kafka-up       # Starts the kafka + ngrok services within the container, make sure the container is started before starting kafka + ngrok
                            # This also starts the consumer automatically as well.
./control.sh kafka-down     # Stops the kafka + ngrok services, your local dev docker container will still be running
./control.sh stop           # Does a `docker-compose down`
./control.sh shell          # Does a `docker exec -it ngradar_website_service bash` this allows you to enter the container shell and run commands inside        
                            # the container if needed
./control.sh log            # Does a `docker logs -f ngradar_website_service`
./control.sh attach         # Does a `docker attach ngradar_website_service`
./control.sh load-staging-data   # builds/runs a one-off container to seed demo db data to your local db
./control.sh hard-reset      # (destructive) removes all containers/images/volumes for this prototype AND rebuilds + starts your docker container from scratch
```


## Summary of Containers:
For local dev, we have the following services spun up in local Docker containers which you will see using Docker Desktop:
    a. Our website that you can visit via localhost in your preferred brower.
    b. A local postgreSQL database and you can connect to to it using DBeaver
    c. When you do a ./control.sh kafka-up you start the following:
        1. ZooKeeper
        2. Kafka broker
        3. Kafka UI
        5. ngrok 
        6. consumer.py
    

