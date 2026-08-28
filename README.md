pytest -s test_submit_waveform ngRadar_Website/tests/test_views.py
pytest ngRadar_Website/tests/test_latencyv2.py 

https://github.com/desmond-L-NRAO/forked-dmspc-system-communication-for-render.git

pytest ngRadar_Website/tests/test_views.py::test_submit_waveform


# dmspc-system-communication
Code for prototyping system communication for the ngRadar project. This code is written by the Data Management & Software Prototyping Cohort (DMSPC).


# How to run locally:
| File | Purpose |
| :---: | :---: |
| `Dockerfile` | Builds the app image: installs Python deps, collects Django static files, runs the dev server |
| `Docker-compose.yml file.` | Orchestrates the app container + a Postgres container on a shared network |
| `load_staging_data.Dockerfile` | A separate, one-off image used only to dump/load staging data from our demo db to the local DB (for local development only) |
| `control.sh` | A thin wrapper script so the team doesn't need to memorize `docker compose` invocations |
| `.env` | Environment variables consumed by `/settings.py` (our .env file is never committed to Github - please ask team for the .env file for local dev) |

# Create a Virtual Environment
# python3 -m venv .venv


# Start the Virtual Environment
# source .venv/bin/activate


## For your convenience, use the below `control.sh` wrapper controls:

These `control.sh` wrap common operations.
Run these commands in your terminal to accomplish any of the following:
```
./control.sh start          # Does a `docker compose up -d`
                            # ^ if you get an error, make sure your Docker Desktop is open
                            
./control.sh rebuild        # Does a `docker compose up -d --build` so it starts + rebuilds image

./control.sh system-up      # Starts the Kafka (kafka-up) and sim (sims-up) services within the container, make sure the container is started before running
                            # This ensures that the containers spin up in the correct order

./control.sh kafka-up       # One component of ./control.sh system-up. Starts the kafka + ngrok services within the container

./control.sh sims-up        # One component of ./control.sh system-up. Starts the sims (GBT, VLBA, and DSOC) and the e-transfer daemon

./control.sh system-down    # Stops both the kafka (kafka-down) and sim (sims-down) services in the correct order; your local dev docker container will still be running

./control.sh kafka-down     # Stops the kafka + ngrok services; your local dev docker container will still be running

./control.sh sims-down      # Stops the sims services; your local dev docker container will still be running

./control.sh stop           # Does a `docker-compose down`

./control.sh shell          # Does a `docker exec -it ngradar_website_service bash` this allows you to enter the container shell and run commands inside        
                            # the container if needed

./control.sh log            # Does a `docker logs -f ngradar_website_service`

./control.sh attach         # Does a `docker attach ngradar_website_service`

./control.sh load-staging-data   # builds/runs a one-off container to seed demo db data to your local db

./control.sh hard-reset      # (destructive) removes all containers/images/volumes for this prototype AND rebuilds + starts your docker container from scratch

./control.sh testcov        # Calculates unit test coverage and prints the test results in the terminal
```
# Commands Within the Control Shell
python3 manage.py migrate       # Retrieves the latest database migrations and applies them
python3 manage.py makemigrations       # Creates new database changes
python3 manage.py createsuperuser       # Allows for a new website user to be created after hard reset or database deletion


## Summary of Containers:
For local dev, we have the following services spun up in local Docker containers which you will see using Docker Desktop:  
- Our website that you can visit via localhost in your preferred browser.  
- A local postgreSQL database that you can connect to using DBeaver.  
- When you do a ./control.sh system-up you start the following:     
    1. Kafka services:   
            - ZooKeeper  
            - Kafka broker  
            - Kafka UI  
            - Kafka topic initialization  
            - ngrok  
            - SeaweedFS  
    2. Sim services:  
            - E-Transfer daemon  
            - GBT sim  
            - VLBA sim  
            - DSOC sim 

