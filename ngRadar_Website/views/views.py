from django.shortcuts import redirect, render
from django.contrib.auth.decorators import login_required

from django.views.decorators.cache import cache_control

#libraries to get files from the outside directory
import sys
from pathlib import Path

#libraries used for data streaming
import json
from django.http import StreamingHttpResponse, Http404, HttpResponse
import boto3

from ngRadar_Website.enums import Stations
from ngRadar_Website.models.models import ObservatoryEvent, uiEvent, gbtEvent, dsocEvent
from django.contrib import messages
from django.contrib.auth import authenticate, login
from django.db.models import Avg
from confluent_kafka import Producer
import os 
import uuid
from datetime import datetime, timezone 
from dotenv import load_dotenv


load_dotenv(override=True)

#program constants
DATE_TIME_STRING=19


def get_obs_events():
    """Helper function to keep data uniform across view updates"""

    latest_events = ObservatoryEvent.objects.order_by("-event_time")
    
    # Calculate the average latency of the last 20 records
    latest_20 = latest_events[:20]
    avg_latency = latest_20.aggregate(Avg('latency_ms'))['latency_ms__avg'] or 0

    return {
        'latest_events': latest_events,
        'latest_event': latest_events.first() if latest_events else None,
        'gbt_event': latest_events.filter(station=Stations.GBT).order_by('-event_time').first(), # only care about the latest event for home gbt partial
        'dsoc_event': latest_events.filter(station=Stations.DSOC).order_by('-event_time').first(), # only care about the latest event for home dsoc partial
        'avg_latency': round(avg_latency, 2)
    }



# Keep as a placeholder when we develop this feature.
# def create_observation(request):
#     # this is the initial view to load the newObservation page
#     return render(request, 'ngRadar_Website/newObservation.html')

def get_Message_Latency():
    last_message_latency_str = str(ObservatoryEvent.objects.last().latency_ms)
    last_message_time_str = str(ObservatoryEvent.objects.last().event_time)
    last_message_time_str = last_message_time_str[:DATE_TIME_STRING]  # Truncate to first 20 characters
    
    data_to_send = {
        "latency": last_message_latency_str,
        "time_sent": last_message_time_str
    }
    yield f"data: {json.dumps(data_to_send)}\n\n"


def latency_graphing(request):
    response = StreamingHttpResponse(
        get_Message_Latency(),
        content_type="text/event-stream; charset=utf-8"
    )
    response["Cache-Control"] = "no-cache"
    return response



def serve_image(request, event_id):
    # Fetch image file key from ObservatoryEvent table and query the image from the SeaweedFS server
    # to render image in website
    obs_event = ObservatoryEvent.objects.get(uuid=event_id)

    s3 = boto3.client(
        's3',
        endpoint_url=os.environ.get('WEED_S3_DOMAIN'),
        aws_access_key_id=os.environ.get('WEED_S3_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('WEED_S3_SECRET_KEY')
    )
    
    response = s3.get_object(
        Bucket=os.environ.get('WEED_S3_BUCKET'),
        Key=obs_event.image_key
    )
    return HttpResponse(
        response["Body"].read(),
        content_type=response["ContentType"],
    )


    

# Need a function AND another partial template for handling the user inputted payload
def submit_waveform(request):
    if request.method == "POST":
        uuid_input = uuid.uuid4()
        waveform  = request.POST.get('waveform')
        timestamp = datetime.now(timezone.utc)
        # Database version
        ui_Event = uiEvent.objects.create(
            uuid = uuid_input,
            selected_waveform = waveform,
            event_time = timestamp
        )
        p = Path("../../../out/ngrok_endpoint.env")
        text = p.read_text().strip()

        bootstrap = None
        for line in text.splitlines():
            if line.startswith("BOOTSTRAP_SERVER="):
                bootstrap = line.split("=", 1)[1].strip()
                break

        if not bootstrap:
            raise RuntimeError("BOOTSTRAP_SERVER not found in /out/ngrok_endpoint.env")
        
        # Kafka version 
        topic = "user_input"
        config = {
            "bootstrap.servers": bootstrap,
            "message.max.bytes": 8388608,
            "client.id": "ui-producer"}
        message = "User input a new waveform."

        def produce(topic, config, key, value):
            producer = Producer(config)
            producer.produce(topic, key=key, value=value)
            
            producer.flush()

        def main():
            key = uuid_input.hex  # Use the UUID as the key for the Kafka message
            value = json.dumps(message).encode("utf-8")
            produce(topic, config, key, value)
        main()
        
    return redirect('home')


#====================================================
# Render the templates
#====================================================


@cache_control(no_cache=True, must_revalidate=True, no_store=True) #Desmond's Auth token fix - comment if we decide not to use
def login_view(request):
    if request.method == 'POST':
        username_input = request.POST['username']
        password_input = request.POST['password']
        
        # This automatically uses the Argon2 settings to verify the password string
        user = authenticate(request, username=username_input, password=password_input)
        
        if user is not None:
            login(request, user)
            return redirect('index')
        else:
            messages.error(request, "Invalid username or password.")
            return render(request, 'registration/login.html')
        
    # Tung's auth token fix - uncomment if we decide to use this
    # response = render(request, 'registration/login.html')
    # response['Cache-Control'] = 'no-cache, no-store, must-revalidate'

    return render(request, 'registration/login.html')


@login_required
def home_view(request):
    # this is the view for the homepage, which will display the most recent observatory event
    latest_events = get_obs_events()['latest_events']
    most_recent_event = latest_events.first() if latest_events else None
    context = {
        'latest_event': most_recent_event
    }
    return render(request, 'ngRadar_Website/home.html', context)


@login_required
def dashboard_view(request):
    return render(
        request,
        "ngRadar_Website/dashboard.html",
        get_obs_events(),
    )


def event_table_partial(request):
    # this is the partial template view for updating the observatory events table
    return render(
        request,
        "ngRadar_Website/partials/dashboard_updates.html",
        get_obs_events(),
    )


def dsoc_event_partial(request, event_id):
    # this is the partial template view for latest dsoc event images on home page
    return render(
        request,
        "ngRadar_Website/partials/dsoc_home_partial.html",
        get_obs_events(), 
    )


def gbt_event_partial(request):
    # this is the partial template view for latest gbt event data on home page
    return render(
        request,
        "ngRadar_Website/partials/gbt_home_partial.html",
        get_obs_events(),
    )