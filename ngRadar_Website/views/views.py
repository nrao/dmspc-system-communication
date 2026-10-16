import asyncio
import json
import logging
import os
import time

from datetime import datetime, timezone

from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required, login_not_required
from django.core.cache import cache
from django.db.models import Avg
from django.http import (
    HttpResponse,
    HttpResponseNotFound,
    JsonResponse,
    StreamingHttpResponse,
)
from django.shortcuts import (
    get_object_or_404,
    redirect,
    render,
)
from django.views.decorators.cache import cache_control
from django.views.decorators.http import require_GET, require_POST

from ngRadar_Website.enums import Message, Stations, Status
from ngRadar_Website.models.models import ObservatoryEvent
from ngRadar_Website.sse import sse_broker
from ngRadar_Website.utils import (
    bootstrap,
    create_s3_client,
    send_kafka_message,
    write_transfer_progress,
    create_presigned_url,
)


logger = logging.getLogger(__name__)


# ============================================================
# Constants
# ============================================================

RECORDS_TO_DISPLAY = 10


# ============================================================
# ObservatoryEvent query helpers
# ============================================================

def get_latest_station_event(station):
    """
    Return the latest persisted ObservatoryEvent for a station.
    Used only to establish initial page state.
    """

    return (
        ObservatoryEvent.objects
        .filter(station=station)
        .order_by("-event_time", "-uuid")
        .first()
    )


def get_latest_image_event():
    """
    Return the most recent event with a SeaweedFS image.
    """

    return (
        ObservatoryEvent.objects
        .exclude(image_key__isnull=True)
        .exclude(image_key="")
        .order_by("-event_time", "-uuid")
        .first()
    )

def get_latest_image_events():
    """
    Return the most recent events with a SeaweedFS image (from 10 vlba etransfers).
    """

    vlba_stations = [
        Stations.HN,
        Stations.LA,
        Stations.BR,
        Stations.OV,
        Stations.PT,
        Stations.KP,
        Stations.SC,
        Stations.FD,
        Stations.NL,
        Stations.MK,]

    events = [] 

    for station in vlba_stations:
        event = (
            ObservatoryEvent.objects
            .filter(rcvr_station=station)
            .exclude(image_key__isnull=True)
            .exclude(image_key="")
            .order_by("-event_time", "-uuid")
            .first()
        )
        if event:
            events.append(event)

    return events


def get_current_waveform():
    """
    Return the most recently submitted waveform.

    UI_EVENT messages are now persisted to ObservatoryEvent
    by db_consumer, so there is no longer a uiEvent table.
    """

    event = (
        ObservatoryEvent.objects
        .filter(station=Stations.UI)
        .exclude(tx_waveform__isnull=True)
        .order_by("-event_time", "-uuid")
        .first()
    )

    return event.tx_waveform if event else None


def get_home_context():
    """
    Initial/fallback state for home.html.

    After initial page load, live operational updates should
    arrive through Kafka -> UI consumer -> SSE.
    """

    latest_event = (
        ObservatoryEvent.objects
        .order_by("-event_time", "-uuid")
        .first()
    )

    return {
        "latest_event": latest_event,
        "ui_event": get_latest_station_event(Stations.UI),
        "gbt_event": get_latest_station_event(Stations.GBT),
        "vlba_event": get_latest_station_event(Stations.HN),
        "dsoc_event": get_latest_station_event(Stations.DSOC),
        "current_waveform": get_current_waveform(),
        "latest_image_event": get_latest_image_event(),
        "latest_image_events": get_latest_image_events(),
    }


def get_dashboard_context(message_number=None):
    """
    Persisted history for dashboard.html.

    ObservatoryEvent is the only source of truth here.
    """

    # use the default value if none is specified from the button
    if message_number is not None:
        records_to_display=message_number
    elif(message_number==None):
        records_to_display=RECORDS_TO_DISPLAY

    latest_events = list(
        ObservatoryEvent.objects
        .order_by("-event_time", "-uuid")
        [:records_to_display]
    )

    avg_latency = (
        ObservatoryEvent.objects
        .exclude(latency_ms=0)
        .aggregate(avg=Avg("latency_ms"))
        ["avg"]
        or 0
    )

    latest_event = (
        latest_events[0]
        if latest_events
        else None
    )

    current_transfer_uuid = (
        latest_event.transfer_uuid
        if latest_event
        else None
    )

    transfer_events = []

    if current_transfer_uuid:
        transfer_events = list(
            ObservatoryEvent.objects
            .filter(
                transfer_uuid=current_transfer_uuid
            )
            .order_by("-event_time", "-uuid")
        )

    transferring_count = (
        ObservatoryEvent.objects
        .filter(
            transfer_uuid=current_transfer_uuid,
            status=Status.TRANSFERRING,
        )
        .count()
        if current_transfer_uuid
        else 0
    )

    return {
        "latest_events": latest_events,
        "latest_event": latest_event,
        "avg_latency": round(avg_latency, 2),
        "current_waveform": get_current_waveform(),
        "latest_image_event": get_latest_image_event(),
        "transfer_events": transfer_events,
        "transfer_resumed": transferring_count > 1,
    }


# ============================================================
# Main Kafka -> browser SSE stream
# ============================================================

async def sse_stream(request):
    """
    Single browser SSE connection.

    Events are published here by the website Kafka consumer.

    Examples:
        gbt_changed
        vlba_changed
        dsoc_changed
        progress changed
        status_changed
        observatory_event_created
        heartbeat
    """

    subscriber = sse_broker.subscribe()
    _, queue = subscriber

    async def event_generator():
        try:
            # Browser should reconnect after 3 seconds
            # if connection is interrupted.
            yield "retry: 3000\n\n"

            while True:
                try:
                    event = await asyncio.wait_for(
                        queue.get(),
                        timeout=15,
                    )

                    event_type = event["type"]
                    event_data = event.get(
                        "data",
                        {},
                    )

                    yield (
                        f"event: {event_type}\n"
                        f"data: {json.dumps(event_data)}\n\n"
                    )

                except asyncio.TimeoutError:
                    heartbeat = {
                        "timestamp": (
                            datetime.now(timezone.utc)
                            .isoformat()
                        )
                    }

                    yield (
                        "event: heartbeat\n"
                        f"data: {json.dumps(heartbeat)}\n\n"
                    )

        except asyncio.CancelledError:
            # Browser navigated away, refreshed,
            # or closed the connection.
            raise

        finally:
            sse_broker.unsubscribe(
                subscriber
            )

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )

    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"

    return response

# ============================================================
# SeaweedFS image serving
# ============================================================

def serve_image(request, uuid):
    event = get_object_or_404(
        ObservatoryEvent,
        uuid=uuid,
    )

    if not event.image_key:
        return HttpResponseNotFound(
            "Image not available."
        )
   

    try:
        presigned_url = create_presigned_url(event)
        return redirect(presigned_url)

    except Exception as exc:
        logger.exception(
            "Failed to retrieve image "
            "from SeaweedFS."
        )

        return HttpResponse(
            "Unable to retrieve image.",
            status=503,
        )


# ============================================================
# Waveform submission lock
# ============================================================

def get_submission_lock_status():
    """Return the current submission-lock status."""
    lock_time = cache.get("submit_locked")

    if lock_time is None:
        return {"locked": False, "error": False}

    observation_completed = (
        ObservatoryEvent.objects
        .filter(
            station=Stations.DSOC,
            status=Status.COMPLETED,
            event_time__gt=lock_time,
        )
        .exists()
    )

    if observation_completed:
        cache.delete("submit_locked")
        return {"locked": False, "error": False}

    return {"locked": True, "error": False}


@require_GET
def lock_status(request):
    try:
        return JsonResponse(get_submission_lock_status())
    except Exception:
        logger.exception("Unable to determine waveform submission lock.")
        return JsonResponse(
            {
                "locked": True,
                "error": True,
                "message": "Unable to determine lock status.",
            },
            status=503,
        )

# ============================================================
# UI -> Kafka waveform submission
# ============================================================
@login_required
@require_POST
def submit_waveform(request):
    try:
        lock_status_data = get_submission_lock_status()
    except Exception:
        logger.exception("Unable to determine waveform submission lock.")
        return JsonResponse(
            {
                "error": True,
                "message": "Unable to determine lock status.",
            },
            status=503,
        )

    if lock_status_data["locked"]:
        return JsonResponse(
            {
                "error": True,
                "message": "A waveform is already being processed.",
            },
            status=409,
        )

    waveform = request.POST.get("waveform")
    user = request.user.username

    if not waveform:
        return JsonResponse(
            {
                "error": True,
                "message": "Waveform is required.",
            },
            status=400,
        )

    producer_topic, producer_config = (
        bootstrap(Stations.UI)
    )

    event_uuid = send_kafka_message(
        message_type=Message.UI_EVENT,

        producer_topic=producer_topic,
        producer_config=producer_config,
        waveform_requester=user,

        station=Stations.UI,

        tx_waveform=waveform,
        rec_waveform=waveform,

        message=(f"{user} submitted waveform {waveform}."),
    )

    if event_uuid is None:
        return JsonResponse(
            {
                "error": True,
                "message": "Unable to submit waveform.",
            },
            status=503,
        )

    cache.set(
        "submit_locked",
        datetime.now(timezone.utc),
    )

    return JsonResponse(
        {
            "error": False,
            "event_uuid": str(event_uuid),
            "message": (
                f"Waveform {waveform} submitted."
            ),
        },
        status=202,
    )


# ============================================================
# Authentication
# ============================================================

@login_not_required
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
        username_input = request.POST[
            "username"
        ]

        password_input = request.POST[
            "password"
        ]

        user = authenticate(
            request,
            username=username_input,
            password=password_input,
        )

        if user is not None:
            login(
                request,
                user,
            )

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


# ============================================================
# Full-page views
# ============================================================

@cache_control(
    no_cache=True,
    must_revalidate=True,
    no_store=True,
    max_age=0,
)
def home_view(request):
    """
    Initial Home render.

    DB provides initial/fallback state.
    Kafka/SSE provides live updates after page load.
    """

    return render(
        request,
        "ngRadar_Website/home.html",
        get_home_context(),
    )


@cache_control(
    no_cache=True,
    must_revalidate=True,
    no_store=True,
    max_age=0,
)
def dashboard_view(request):
    """
    Dashboard represents persisted ObservatoryEvent history.
    """
    #handle requests made from drop down button
    if request.method == 'POST':
        message_number = int(request.POST.get('message_number',RECORDS_TO_DISPLAY))
        #save number from button through page reloads
        request.session['message_number'] = message_number
    else:
        message_number = int(request.session.get('message_number', RECORDS_TO_DISPLAY))

    #send message number back to function
    context = get_dashboard_context(message_number=message_number)

    context['selected_number'] = message_number

    return render(
        request,
        "ngRadar_Website/dashboard.html",
        context,
    )


# ============================================================
# HTMX partials
# ============================================================

@require_GET
def event_table_partial(request):
    """
    Refresh after observatory_event_created SSE event.

    This reads committed ObservatoryEvent rows only.
    """

    message_number = int(request.session.get('message_number', RECORDS_TO_DISPLAY))

    return render(
        request,
            "ngRadar_Website/partials/dashboard_updates.html",
            get_dashboard_context(message_number=message_number),
    )


# ============================================================
# latency graph 
# ============================================================
@require_GET
def latency_data(request):
    
    message_number = int(request.session.get("message_number", RECORDS_TO_DISPLAY))

    database_events = (
        ObservatoryEvent.objects
        .exclude(tx_waveform="Tx_OFF")
        .order_by("-event_time")
        [:message_number]
    )

    latest_events = list(
        reversed(database_events)
    )

    latency_array = []
    event_source_array = []
    event_metadata_array = []

    for event in latest_events:
        latency_array.append(
            round(event.latency_ms, 3)
        )

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

        event_source_array.append(
            station_short
        )

        event_metadata_array.append({
            "station": station_full,
            "status": (
                event.get_status_display()
                if event.status is not None
                else "-"
            ),
            "time": (
                event.event_time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            ),
            "object_id": (
                event.object_id or "-"
            ),
            "target": (
                event.target or "-"
            ),
        })

    return JsonResponse({
        "latency_array": latency_array,
        "event_source_array":
            event_source_array,
        "event_metadata_array":
            event_metadata_array,
    })