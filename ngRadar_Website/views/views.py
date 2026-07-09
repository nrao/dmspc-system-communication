from django.shortcuts import redirect, render
from django.contrib.auth.decorators import login_required

from django.views.decorators.cache import cache_control

#libraries to get files from the outside directory
import sys
from pathlib import Path

#libraries used for data streaming
import json
from django.http import StreamingHttpResponse, HttpResponse, Http404

from ngRadar_Website.models.models import ObservatoryEvent, uiEvent, gbtEvent
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
        
    response = render(request, 'registration/login.html')
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        
    return response 


#import the producer
outside_dir = str(Path(__file__).resolve().parents[2])
sys.path.append(outside_dir)

@login_required #ensure login is required and users can't access any other web address directly
def get_homepage_index(request):
    # this is the initial view to load the homepage
    return render(request, 'ngRadar_Website/index.html')


def get_dashboard_context():
    """Helper function to keep data uniform across view updates"""
    latest_events = ObservatoryEvent.objects.all().order_by('-event_time')[:20]
    
    # Calculate the average latency of the last 20 records in Postgres
    avg_latency = latest_events.aggregate(Avg('latency_ms'))['latency_ms__avg'] or 0.0

    # Calculate anything else we need for the initial load of the dashboard
    
    return {
        'events': latest_events,
        'avg_latency': round(avg_latency, 2)
    }


def get_latest_event():
      latest_event = ObservatoryEvent.objects.last()
      return {'latest_event': latest_event}

def live_dashboard(request):
    # this is the initial view to load the live dashboard
    context = get_latest_event()
    return render(request, 'ngRadar_Website/index.html', context)

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

def dashboard_view(request):
    context = get_dashboard_context()
    return render(request, 'ngRadar_Website/dashboard.html', context) # pass any other vars to frontend here

def event_table_partial(request):
    # this is the partial template view for updating the observatory events table
    context = get_dashboard_context()

    return render(request, 'ngRadar_Website/partials/dashboard_updates.html', context)

def serve_image(request, event_id):

    # get draw bytes from DB 
    raw = ObservatoryEvent.objects.filter(id=event_id).values_list('image_file', flat=True).first()

    # if no image found, show 404
    if not raw: 
        raise Http404
    
    # send raw bytes to browser, labeled as PNG 
    return HttpResponse(bytes(raw), content_type ='image/png')

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
        
    return redirect('index')