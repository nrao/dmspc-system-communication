from django.shortcuts import redirect, render, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.views.decorators.cache import cache_control


#libraries used for data streaming
import json
from django.http import StreamingHttpResponse, JsonResponse, HttpResponse

# serve_image imports
from ngRadar_Website.utils import create_s3_client # , get_presigned_url

#libraries used for lock status
from django.core.cache import cache

from ngRadar_Website.models.models import ObservatoryEvent, uiEvent, ngrok_endpoint, gbtEvent, dsocEvent
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout, logout
from django.db.models import Avg
from confluent_kafka import Producer
import uuid
import os
from datetime import datetime, timezone 
# from ngRadar_Website.utils import views_bootstrap

# views_bootstrap()

#program constants

RECORDS_TO_DISPLAY=20
LAST_RECORDS = 5
EXPIRE_TIME_SECONDS = 3600

def get_obs_events():
    """Helper function to keep data uniform across view updates"""

    latest_events = ObservatoryEvent.objects.order_by("-event_time")[:RECORDS_TO_DISPLAY]
    ui_events = uiEvent.objects.order_by("-event_time")[:LAST_RECORDS]
    gbt_events = gbtEvent.objects.order_by("-event_time")[:LAST_RECORDS]
    dsoc_events = dsocEvent.objects.order_by("-event_time")[:LAST_RECORDS]
    avg_latency = latest_events.aggregate(Avg('latency_ms'))['latency_ms__avg'] or 0
    current_waveform = ui_events.first().selected_waveform if ui_events.exists() else None

    return {
        'latest_events': latest_events,
        'latest_event': latest_events.first() if latest_events else None,
        'ui_event': ui_events.first() if ui_events else None,
        'gbt_event': gbt_events.first() if gbt_events else None,
        'dsoc_event': dsoc_events.first() if dsoc_events else None,
        'avg_latency': round(avg_latency, 2),
        'current_waveform': current_waveform
    }


# Keep as a placeholder when we develop this feature.
# def create_observation(request):
#     # this is the initial view to load the newObservation page
#     return render(request, 'ngRadar_Website/newObservation.html')

def get_Message_Latency():
    #create empty arrays for message latency and time
    message_latency_arr=[]
    message_time_arr=[]
    database_events = ObservatoryEvent.objects.order_by("-event_time")
    latest_events = database_events[:RECORDS_TO_DISPLAY]

    for object in latest_events: #loop will
        unformatted_date_time = str(object.event_time)
        formatted_date_time = unformatted_date_time[0:10], unformatted_date_time[11:19]#format the time in the views rather than in the front end
        
        # Prevent the tx off messages from being displayed since they do not have latency
        if(str(object.tx_waveform)!= "Tx_OFF"):
            message_latency_arr.append(str(round(object.latency_ms,3)))#round the latency to 3 decimal places
            message_time_arr.append(formatted_date_time)
    
    data_to_send = {
        "latency_array": message_latency_arr,
        "time_sent_array": message_time_arr
    }
    yield f"data: {json.dumps(data_to_send)}\n\n"


def latency_graphing(request):
    response = StreamingHttpResponse(
        get_Message_Latency(),
        content_type="text/event-stream; charset=utf-8"
    )
    response["Cache-Control"] = "no-cache"
    return response


def serve_image(request, uuid):
    event = get_object_or_404(ObservatoryEvent, uuid=uuid)

    bucket = bucket = os.environ["WEED_S3_BUCKET"]

    s3 = create_s3_client()

    # presigned_url = get_presigned_url(s3, event)
    # return redirect(presigned_url)

    obj = s3.get_object(
    Bucket=bucket,
    Key=event.image_key,
    )

    return HttpResponse(
        obj["Body"].read(),
        content_type=obj["ContentType"],
    )




# Function for lock down user
# Return True if event time is greater than lock time 
# Othere wise False  
def lock_status(request):
    lock_time = cache.get('submit_locked', None)
    if lock_time is None:
        return JsonResponse({"locked": False})
    elif ObservatoryEvent.objects.filter(event_time__gt=lock_time, image_key__isnull=False):
        cache.delete('submit_locked')
        return JsonResponse({'locked':False})
    return JsonResponse({'locked':True})

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

        # p = Path("../../../out/ngrok_endpoint.env")
        # text = p.read_text().strip()

        # bootstrap = None
        # for line in text.splitlines():
        #     if line.startswith("BOOTSTRAP_SERVER="):
        #         bootstrap = line.split("=", 1)[1].strip()
        #         break

        # if not bootstrap:
        #     raise RuntimeError("BOOTSTRAP_SERVER not found in /out/ngrok_endpoint.env")
        
        bootstrap = ngrok_endpoint.objects.last().bootstrap

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
        
        # add a cache for submit time
        cache.set('submit_locked', datetime.now(timezone.utc))
    return redirect('home')


#====================================================
# Render the templates
#====================================================


@cache_control(no_cache=True, must_revalidate=True, no_store=True, max_age=0) #Desmond's Auth token fix - comment if we decide not to use
def login_view(request):
    #logout_view(request)
    
    if(request.user.is_authenticated):#will log the user out if they come to the login page and are still logged in
        return logout_view(request)#goes to logout message


    if request.method == 'POST':
        username_input = request.POST['username']
        password_input = request.POST['password']
        
        # This automatically uses the Argon2 settings to verify the password string
        user = authenticate(request, username=username_input, password=password_input)
        
        if user is not None:
            login(request, user)
            return redirect('home')
        else:
            messages.error(request, "Invalid username or password.")
            return render(request, 'registration/login.html')
        
    # Tung's auth token fix - uncomment if we decide to use this
    # response = render(request, 'registration/login.html')
    # response['Cache-Control'] = 'no-cache, no-store, must-revalidate'

    return render(request, 'registration/login.html')

#@cache_control(no_cache=True, must_revalidate=True, no_store=True, max_age=0)
def logout_view(request):
    logout(request)
    response = logging_out_message(request)
    return response


#@cache_control(no_cache=True, must_revalidate=True, no_store=True, max_age=0)
def logging_out_message(request):
    return render(request, 'ngRadar_Website/partials/log_out_partial.html')


@cache_control(no_cache=True, must_revalidate=True, no_store=True, max_age=0)
@login_required
def home_view(request):

    response = render(request, "ngRadar_Website/home.html", get_obs_events())
    #Need to add cache control modifiers here
    return response


@cache_control(no_cache=True, must_revalidate=True, no_store=True, max_age=0)
@login_required
def dashboard_view(request):

    response = render(request, "ngRadar_Website/dashboard.html", get_obs_events())
    #Need to add cache control modifiers here
    return response


def event_table_partial(request):
    # this is the partial template view for updating the observatory events table
    return render(
        request,
        "ngRadar_Website/partials/dashboard_updates.html",
        get_obs_events(),
    )


def status_partial(request):
    # this is the partial template view for the status box on the home page

    return render(
        request,
        "ngRadar_Website/partials/status_partial.html",
        get_obs_events(),
    )


def dsoc_event_partial(request):
    # this is the partial template view for latest dsoc event image on home page

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

