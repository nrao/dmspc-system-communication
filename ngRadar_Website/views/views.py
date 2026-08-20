# auth imports
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render, get_object_or_404
from django.views.decorators.cache import cache_control
from django.views.decorators.http import require_POST, require_GET

#libraries used for data streaming
from django.http import StreamingHttpResponse, JsonResponse, HttpResponse, HttpResponseNotFound

# serve_image imports
from ngRadar_Website.utils import create_s3_client, bootstrap, write_transfer_progress #get_presigned_url
from ngRadar_Website.enums import Stations, Message

#libraries used for lock status
from django.core.cache import cache

from ngRadar_Website.models.models import ObservatoryEvent, uiEvent, gbtEvent, dsocEvent, ETransferEvent
from ngRadar_Website.models.models import ObservatoryEvent, uiEvent, gbtEvent, dsocEvent, ETransferEvent
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout, logout
from django.db.models import Avg
from datetime import datetime, timezone

from ngRadar_Website.utils import produce

import json, uuid, os, time


#program constants
RECORDS_TO_DISPLAY=20
LAST_RECORDS = 5
EXPIRE_TIME_SECONDS = 3600

def get_obs_events():
    """Helper function to keep data uniform across view updates"""

    latest_events = ObservatoryEvent.objects.order_by(
        "-event_time",
        "-uuid",
        )[:RECORDS_TO_DISPLAY]
    ui_events = uiEvent.objects.order_by("-event_time")[:LAST_RECORDS]
    gbt_events = gbtEvent.objects.order_by("-event_time")[:LAST_RECORDS]
    dsoc_events = dsocEvent.objects.order_by("-event_time")[:LAST_RECORDS]
    avg_latency = latest_events.aggregate(Avg('latency_ms'))['latency_ms__avg'] or 0
    current_waveform = ui_events.first().selected_waveform if ui_events.exists() else None
    latest_etr_events = ETransferEvent.objects.order_by("-event_time")[:RECORDS_TO_DISPLAY]
    current_transfer_uuid = latest_events.first().transfer_uuid if latest_events.exists() else None
    latest_etr_event = ETransferEvent.objects.filter(transfer_uuid=current_transfer_uuid).order_by("-event_time").first()
    latest_image_event = (
            ObservatoryEvent.objects
            .exclude(image_key__isnull=True)
            .exclude(image_key="")
            .order_by("-event_time")
            .first()
        )

    return {
        'latest_events': latest_events,
        'latest_event': ObservatoryEvent.objects.order_by("-event_time").first() if latest_events else None,
        'ui_event': ui_events.first() if ui_events else None,
        'gbt_event': gbt_events.first() if gbt_events else None,
        'dsoc_event': dsoc_events.first() if dsoc_events else None,
        'avg_latency': round(avg_latency, 2),
        'current_waveform': current_waveform,
        'latest_etr_events': latest_etr_events,
        'latest_etr_event': latest_etr_event,
        'latest_image_event': latest_image_event,
    }


def get_Message_Latency():
    database_events = (
        ObservatoryEvent.objects
        .exclude(tx_waveform="Tx_OFF")
        .order_by("-event_time")[:RECORDS_TO_DISPLAY]
    )

    # so it will read left to right in the graph, we need to reverse the order of the events
    latest_events = list(reversed(database_events))

    latency_array = []
    event_source_array = []
    event_metadata_array = []

    for event in latest_events:
        latency_array.append(round(event.latency_ms, 3))


        station_short = (
            Stations(event.station).name
            if event.station is not None
            else "Unknown"
        )

        station_full = (
            event.get_station_display()
            if event.station is not None
            else "Unknown"
        )

        event_source_array.append(station_short)

        event_metadata_array.append({
            "station": station_full,
            "status": (
                event.get_status_display()
                if event.status is not None
                else "-"
            ),
            "time": event.event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "object_id": event.object_id or "-",
            "target": event.target or "-",
        })

    data_to_send = {
        "latency_array": latency_array,
        "event_source_array": event_source_array,
        "event_metadata_array": event_metadata_array,
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

    bucket = os.environ["WEED_S3_BUCKET"]

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
    elif dsocEvent.objects.filter(event_time__gt=lock_time).exists():
        cache.delete('submit_locked')
        return JsonResponse({'locked':False})
    return JsonResponse({'locked':True})


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
        
        # bootstrap = ngrok_endpoint.objects.last().bootstrap

        topic, config = bootstrap(Stations.UI)

        # Kafka version 
        # topic = "user_input"
        # config = {
        #     "bootstrap.servers": bootstrap,
        #     "message.max.bytes": 8388608,
        #     "client.id": "ui-producer"}
        # message = "User input a new waveform."

        def main():
            key = str(Message.UI_EVENT)
            value = uuid_input.hex  # Use the UUID as the value for the Kafka message
            produce(topic, config, key, value)
            write_transfer_progress(received_bytes=0, total_bytes=0, percent=0.0, transfer_id=0)  # Reset the progress bar after sending the message
        main()
        
        # add a cache for submit time
        cache.set('submit_locked', datetime.now(timezone.utc))
    return redirect('home')

#====================================================
# Render the templates
#====================================================

@cache_control(
    no_cache=True,
    must_revalidate=True,
    no_store=True,
    max_age=0,
)
def login_view(request):
    if request.user.is_authenticated:
        return redirect("home")

    if request.method == "POST":
        username_input = request.POST["username"]
        password_input = request.POST["password"]

        user = authenticate(
            request,
            username=username_input,
            password=password_input,
        )

        if user is not None:
            login(request, user)
            return redirect("home")

        messages.error(
            request,
            "Invalid username or password.",
        )

    return render(
        request,
        "registration/login.html",
    )


@require_POST
def logout_view(request):
    logout(request)
    return redirect("login")


@cache_control(no_cache=True, must_revalidate=True, no_store=True, max_age=0)
@login_required
def home_view(request):

    response = render(request, "ngRadar_Website/home.html", get_obs_events())
    return response


@cache_control(no_cache=True, must_revalidate=True, no_store=True, max_age=0)
@login_required
def dashboard_view(request):

    response = render(request, "ngRadar_Website/dashboard.html", get_obs_events())
    return response


@login_required
def event_table_partial(request):
    # this is the partial template view for updating the observatory events table
    return render(
        request,
        "ngRadar_Website/partials/dashboard_updates.html",
        get_obs_events(),
    )


@login_required
def status_partial(request):
    # this is the partial template view for the status box on the home page

    return render(
        request,
        "ngRadar_Website/partials/status_partial.html",
        get_obs_events(),
    )


@login_required
def dsoc_event_partial(request):
    # this is the partial template view for latest dsoc event image on home page

    return render(
        request,
        "ngRadar_Website/partials/dsoc_home_partial.html",
        get_obs_events(),
    )


@login_required
def gbt_event_partial(request):
    # this is the partial template view for latest gbt event data on home page
    return render(
        request,
        "ngRadar_Website/partials/gbt_home_partial.html",
        get_obs_events(),
    )


PROGRESS_JSON_PATH = "/service/mock_assets/progress.json"  # <-- endpoint to stream to front end for progress bar. progress.json is updated by etc_send() while the VLBA e-transfer is occurring.

@login_required
@require_GET
def progress_sse(request):
    if not os.path.exists(PROGRESS_JSON_PATH):
        return HttpResponseNotFound("Progress file not found")

    def sse(event=None, data=None):
        out = ""
        if event:
            out += f"event: {event}\n"
        if data is not None:
            out += f"data: {data}\n"
        return out + "\n"

    def gen():
        last_seen = None  # last full progress payload
        last_transfer_id = None

        while True:
            if not os.path.exists(PROGRESS_JSON_PATH):
                time.sleep(0.5)
                continue

            try:
                with open(PROGRESS_JSON_PATH, "r", encoding="utf-8") as f:
                    payload = json.load(f)

                received = payload.get("received_bytes", 0)
                total = payload.get("total_bytes", 0)
                percent = payload.get("percent", 0.0)
                transfer_id = payload.get("transfer_id", 0)

                # Always emit when the payload changes (or transfer changes)
                if payload != last_seen:
                    last_seen = payload
                    yield sse(data=json.dumps({
                        "received": received,
                        "total": total,
                        "percent": percent,
                        "transfer_id": transfer_id,
                    }))

                # Emit a done event, but DO NOT break/close the stream
                if total > 0 and received >= total:
                    # Only emit done once per transfer_id
                    if transfer_id != last_transfer_id:
                        last_transfer_id = transfer_id
                        yield sse(event="done", data=json.dumps({
                            "transfer_id": transfer_id,
                            "percent": percent,
                        }))
                    # Wait for next transfer start (transfer_id changes)
                    while True:
                        time.sleep(0.5)
                        if not os.path.exists(PROGRESS_JSON_PATH):
                            continue
                        with open(PROGRESS_JSON_PATH, "r", encoding="utf-8") as f:
                            payload2 = json.load(f)
                        next_transfer_id = payload2.get("transfer_id", 0)
                        if next_transfer_id != transfer_id:
                            last_seen = None
                            break

            except Exception as e:
                yield sse(event="progress_error", data=json.dumps({"message": str(e)}))

            time.sleep(0.2)

    response = StreamingHttpResponse(
        gen(),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response
