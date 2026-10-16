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

./control.sh system-down    # Stops both the kafka (kafka-down) and sim (sims-down) services in the correct order; your local dev docker container will still be running

./control.sh stop           # Does a `docker-compose down`

./control.sh shell          # Does a `docker exec -it ngradar_website_service bash` this allows you to enter the container shell and run commands inside        
                            # the container if needed

./control.sh log            # Does a `docker logs -f ngradar_website_service`

./control.sh attach         # Does a `docker attach ngradar_website_service`

./control.sh hard-reset     # (destructive) removes all containers/images/volumes for this prototype AND rebuilds + starts your docker container from scratch

./control.sh testcov        # Calculates unit test coverage and prints the test results in the terminal

./control.sh droplets-up    # Brings up all Digital Ocean droplets remotely via SSH and more control.sh commands

./control.sh droplets-down  # Brings all droplets back down. Leaves the portainer services running so we can always monitor with the Portainer UI

./control.sh <sim>-up       # When on a droplet, i.e. the dsoc droplet, you can run a dsoc-up to bring up exactly what that droplet needs.

./control.sh portainer-up   # If the dsoc droplet's portainer service fails, start it up with run this command

./control.sh agent-up       # If the gbt or vlba droplet's agent fails, start it up again with this command.
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
            - SeaweedFS  
    2. Sim services:  
            - E-Transfer daemon  
            - GBT sim  
            - VLBA sim  
            - DSOC sim 

