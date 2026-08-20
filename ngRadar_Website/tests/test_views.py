from pathlib import Path
from unittest.mock import patch, MagicMock

from ngRadar_Website.views.views import get_obs_events
from ngRadar_Website.enums import Stations, Message
from datetime import datetime, timezone
from ngRadar_Website.models.models import gbtEvent, dsocEvent, ObservatoryEvent, uiEvent
from django.test import RequestFactory
from django.urls import reverse
from django.http import HttpResponse, HttpResponseRedirect

import json
import uuid


# ==============================================================================
# IMPORTANT:
# Because we read "ngrok_endpoint.env" on import, we need to patch the Path globally
# before importing all the functions we want to test.
# ==============================================================================
mock_env_data = "BOOTSTRAP_SERVER=localhost:9092\nSOME_OTHER_VAR=value" 
with patch("pathlib.Path.read_text", return_value=mock_env_data):
    from ngRadar_Website.views.views import (
        serve_image,
        lock_status,
        submit_waveform,
        login_view,
        logout_view,
        latency_graphing,
        home_view,
        dashboard_view,
        event_table_partial,
        status_partial,
        dsoc_event_partial,
        gbt_event_partial,
    )


"""Commented out is my testing for get_obs_event, which we are no longer testing, but I'm keeping the code for now"""

# #Test the Functions from views.py
# @pytest.mark.django_db
# def test_get_obs_events():

#     now = datetime.now(timezone.utc)

#     for i in range(25):
#         ObservatoryEvent.objects.create(
#             uuid = f"{i}",
#             object_id = f"OBJ{i}",
#             target = "target",
#             tx_waveform = "Sinewave",
#             rec_waveform = "Sinewave",
#             product_type = "DDM",
#             product_id = f"00{i}",
#             station = Stations.GBT,
#             event_time = now - timedelta(seconds=i),
#             created_at = now - timedelta(seconds=i+5),
#             xmit_station = Stations.GBT,
#             rcvr_station = Stations.DSOC,
#             image_key = f"ddm/target/uuid.png",
#             num_bytes = 2048,
#             latency_ms = 100,
#         )
#         gbtEvent.objects.create(
#             uuid = f"{i}",
#             object_id = f"OBJ{i}",
#             target = "target",
#             tx_waveform = "Sinewave",
#             rec_waveform = "Sinewave",
#             event_time = now - timedelta(seconds=i),
#             latency_ms = 100
#         )
#         dsocEvent.objects.create(
#             uuid = f"{i}",
#             object_id = f"OBJ{i}",
#             target = "target",
#             image_key = f"ddm/target/uuid.png",
#             num_bytes = 2048,
#             event_time = now - timedelta(seconds=i),
#             latency_ms = 100
#         )
#         uiEvent.objects.create(
#             uuid = f"{i}",
#             selected_waveform = "Sinewave",
#             event_time = now - timedelta(seconds=i)
#         )

#     theObservatoryEvents = get_obs_events()

#     latest_obs_events = theObservatoryEvents["latest_events"]
#     length = len(latest_obs_events)
#     assert length == 20


# ==============================================================================
# 1. get_message_latency Test
# ==============================================================================

"""Desmond's code here:"""


# ==============================================================================
# 2. serve_image Test
# ==============================================================================


@patch.dict(
    "os.environ",
    {
        "WEED_S3_BUCKET": "fake_bucket"
    },
)
@patch("ngRadar_Website.views.views.get_object_or_404")
@patch("ngRadar_Website.views.views.create_s3_client")
def test_serve_image(mock_create, mock_get_obj):

    mock_event = MagicMock()
    mock_event.image_key = "images/test.png"
    mock_get_obj.return_value = mock_event

    mock_s3 = MagicMock()
    mock_create.return_value = mock_s3
    mock_s3.get_object.return_value = {
        "Body": MagicMock(
            read=MagicMock(return_value=b"fake_image_data")
        ),
        "ContentType": "image/png",
    }

    #call the function:
    output = serve_image(request = "request", uuid = "uuid")

    mock_get_obj.assert_called_once_with(ObservatoryEvent, uuid="uuid")
    mock_create.assert_called_once()
    mock_s3.get_object.assert_called_once_with(
        Bucket="fake_bucket", Key="images/test.png"
    )

# ===============================================================================
# 3. Submit waveform test
# ===============================================================================
@patch("ngRadar_Website.views.views.uuid.uuid4")
@patch("ngRadar_Website.views.views.datetime")
@patch("ngRadar_Website.views.views.produce")
@patch("ngRadar_Website.views.views.cache")
@patch("ngRadar_Website.views.views.write_transfer_progress")
@patch("ngRadar_Website.views.views.uiEvent.objects.create")
def test_submit_waveform(Mock_UI_EVENT, Mock_ProgressBar, Mock_Cache, Mock_Producer, mock_datetime, test_uuid):
    #create simulated data
    mock_uuid = uuid.UUID('12345678-1234-5678-1234-567812345678')
    test_timestamp = datetime(2026, 8, 17, 12, 30, 45, tzinfo=timezone.utc)
    test_waveform = '45'

    #create fixed return values for UUID and date time
    test_uuid.return_value = mock_uuid
    mock_datetime.now.return_value=test_timestamp

    #generate a mock post request
    factory = RequestFactory()
    myRequest = factory.post('home/submit-waveform/', data={'waveform':test_waveform})

    # #mock the bootsrap value
    # mock_ngrok = MagicMock()
    # mock_ngrok.bootstrap = mock_endpoint
    # Mock_bootstrap.return_value = mock_ngrok

    #mock a UI Event
    Mock_EVENT = MagicMock()
    Mock_EVENT.uuid = mock_uuid
    Mock_EVENT.selected_waveform = test_waveform
    Mock_EVENT.event_time = test_timestamp
    Mock_UI_EVENT.return_value = Mock_EVENT

    data = submit_waveform(myRequest)
    
    # Assert MockUiEvent was called
    Mock_UI_EVENT.assert_called_once()

    # Assert waveform_producer was called
    Mock_Producer.assert_called_once()
    
    #get the parameters from the Mock_producer
    waveform_producer_topic = Mock_Producer.call_args[0][0]
    waveform_producer_config = Mock_Producer.call_args[0][1]
    waveform_producer_messageKey = Mock_Producer.call_args[0][2]
    waveform_producer_uuid = Mock_Producer.call_args[0][3]

    #test that data sent in the fake message matches the simulated data
    assert waveform_producer_topic == "user_input"

    # assert waveform_producer_config['bootstrap.servers'] == mock_endpoint
    assert waveform_producer_config['client.id'] == 'ui-producer'

    assert waveform_producer_messageKey == str(Message.UI_EVENT)


    #assert the UUID and convert to hexadecimal
    assert waveform_producer_uuid == mock_uuid.hex

    #assert json.loads(waveform_producer_value.decode('utf-8')) == "User input a new waveform."

    # Assert cache was set
    Mock_Cache.set.assert_called_once()

    #assert call to reset progress bar was made
    Mock_ProgressBar.assert_called_once()

# ==============================================================================
# 4. login_view Test
# ==============================================================================


@patch("ngRadar_Website.views.views.redirect")
def test_login_view_auth(mock_redirect):
    """Scenario 1: user is authenticated"""

    #We need this django function to generate a fake http request for us:
    factory = RequestFactory()
    request = factory.get("/login/")

    request.user = MagicMock()
    request.user.is_authenticated = True

    # Make redirect() return a real response object Django can set headers on
    expected_url = reverse("home")  # adjust if different
    mock_redirect.return_value = HttpResponseRedirect(expected_url)

    response = login_view(request)

    mock_redirect.assert_called_once_with("home")
    assert response.status_code == 302
    assert response.url == expected_url

@patch("ngRadar_Website.views.views.authenticate")
@patch("ngRadar_Website.views.views.login")
@patch("ngRadar_Website.views.views.redirect")
@patch("ngRadar_Website.views.views.logout_view")
def test_login_view_post_valid(mock_logout, mock_redirect, mock_login, mock_auth):
    """Scenario 2: method = POST with valid credentials"""

    factory = RequestFactory()
    request = factory.post("/login/", {
        "username": "name",
        "password": "secret"
    })

    #set auth to False to avoid first if statement:
    request.user = MagicMock()
    request.user.is_authenticated = False

    mock_user = MagicMock()
    mock_auth.return_value = mock_user

    mock_response = HttpResponseRedirect("/home/")
    mock_redirect.return_value = mock_response

    output = login_view(request)

    mock_login.assert_called_once_with(request, mock_user)
    assert output == mock_response
    mock_auth.assert_called_once_with(request, username="name", password="secret")
    mock_logout.assert_not_called()

@patch("ngRadar_Website.views.views.authenticate")
@patch("ngRadar_Website.views.views.render")
@patch("ngRadar_Website.views.views.messages.error")
@patch("ngRadar_Website.views.views.logout_view")
def test_login_view_post_invalid(mock_logout, mock_msg_error, mock_render, mock_auth):
    """Scenario 3: method = POST with IN-valid credentials"""

    factory = RequestFactory()
    request = factory.post("/login/", {
        "username": "name",
        "password": "secret"
    })

    #set auth to False to avoid first if statement:
    request.user = MagicMock()
    request.user.is_authenticated = False

    mock_user = None
    mock_auth.return_value = mock_user

    mock_response = HttpResponse("login page")
    mock_render.return_value = mock_response

    output = login_view(request)

    assert output == mock_response
    mock_auth.assert_called_once_with(request, username="name", password="secret")
    mock_msg_error.assert_called_once_with(request, "Invalid username or password.")
    mock_render.assert_called_once_with(request, 'registration/login.html')
    mock_logout.assert_not_called()


# ==============================================================================
# 5. latency_graphing Test
# ==============================================================================

@patch("ngRadar_Website.views.views.StreamingHttpResponse")
@patch("ngRadar_Website.views.views.get_Message_Latency")
def test_latency_graphing(mock_get_msg, mock_streaming):
    response = MagicMock()
    mock_streaming.return_value = response

    output = latency_graphing("request")

    assert output == response
    mock_streaming.assert_called_once_with(mock_get_msg(), content_type="text/event-stream; charset=utf-8")


# ==============================================================================
# 6. lock_status Test
# ==============================================================================

@patch("ngRadar_Website.views.views.cache.get")
@patch("ngRadar_Website.views.views.JsonResponse")
def test_lock_status_none(mock_json, mock_cache_get):
    """Scenario 1: lock time is None"""
    mock_cache_get.return_value = None

    mock_json.return_value = "fake_json_response"

    output = lock_status("request")

    assert output == "fake_json_response"
    mock_cache_get.assert_called_once_with('submit_locked', None)
    mock_json.assert_called_once_with({'locked':False})


@patch("ngRadar_Website.views.views.cache.get")
@patch("ngRadar_Website.views.views.dsocEvent")
@patch("ngRadar_Website.views.views.cache.delete")
@patch("ngRadar_Website.views.views.JsonResponse")
def test_lock_matching_event_time(mock_json, mock_cache_delete, mock_dsocEvent, mock_cache_get):
    """Scenario 2: lock time matches the event time"""
    mock_cache_get.return_value = "fake_time"

    mock_dsocEvent.objects.filter.return_value.exists.return_value = True

    mock_cache_delete.return_value = None

    mock_json.return_value = "fake_json_response"

    output = lock_status("request")

    assert output == "fake_json_response"
    mock_cache_get.assert_called_once_with('submit_locked', None)
    mock_dsocEvent.objects.filter.assert_called_once_with(event_time__gt="fake_time")
    mock_cache_delete.assert_called_once_with('submit_locked')
    mock_json.assert_called_once_with({'locked':False})


@patch("ngRadar_Website.views.views.cache.get")
@patch("ngRadar_Website.views.views.dsocEvent")
@patch("ngRadar_Website.views.views.JsonResponse")
def test_lock_true(mock_json, mock_dsocEvent, mock_cache_get):
    """Scenario 3: lock status is True"""
    mock_cache_get.return_value = "fake_time"

    mock_dsocEvent.objects.filter.return_value.exists.return_value = False

    mock_json.return_value = "fake_json_response"

    output = lock_status("request")

    assert output == "fake_json_response"
    mock_cache_get.assert_called_once_with('submit_locked', None)
    mock_dsocEvent.objects.filter.assert_called_once_with(event_time__gt="fake_time")
    mock_json.assert_called_once_with({'locked':True})


# ==============================================================================
# 7. logout_view Test
# ==============================================================================
@patch("ngRadar_Website.views.views.redirect")
@patch("ngRadar_Website.views.views.logout")
def test_logout_view(mock_logout, mock_redirect):
    #We need this django function to generate a fake http request for us:
    factory = RequestFactory()
    request = factory.post("/logout/")

    logout_view(request)

    mock_logout.assert_called_once_with(request)
    mock_redirect.assert_called_once_with("login")


# ==============================================================================
# 8. home_view Test
# ==============================================================================

@patch("ngRadar_Website.views.views.render")
@patch("ngRadar_Website.views.views.get_obs_events")
def test_home_view(mock_obs_event, mock_render):
    request = MagicMock()

    response = HttpResponse("fake_response")
    mock_render.return_value = response
    mock_obs_event.return_value = "fake_obs_events"

    output = home_view(request)

    assert output == response
    mock_obs_event.assert_called_once_with()
    mock_render.assert_called_once_with(request, "ngRadar_Website/home.html", mock_obs_event())


# ==============================================================================
# 9. dashboard_view Test
# ==============================================================================

@patch("ngRadar_Website.views.views.render")
@patch("ngRadar_Website.views.views.get_obs_events")
def test_dashboard_view(mock_obs_event, mock_render):
    request = MagicMock()

    response = HttpResponse("fake_response")
    mock_render.return_value = response
    mock_obs_event.return_value = "fake_obs_events"

    output = dashboard_view(request)

    assert output == response
    mock_obs_event.assert_called_once_with()
    mock_render.assert_called_once_with(request, "ngRadar_Website/dashboard.html", mock_obs_event())


# ==============================================================================
# 10. event_table_partial Test
# ==============================================================================

@patch("ngRadar_Website.views.views.render")
@patch("ngRadar_Website.views.views.get_obs_events")
def test_event_table_partial(mock_obs_event, mock_render):
    request = MagicMock()

    response = HttpResponse("fake_response")
    mock_render.return_value = response
    mock_obs_event.return_value = "fake_obs_events"

    output = event_table_partial(request)

    assert output == response
    mock_obs_event.assert_called_once_with()
    mock_render.assert_called_once_with(request, "ngRadar_Website/partials/dashboard_updates.html", mock_obs_event())


# ==============================================================================
# 11. status_partial Test
# ==============================================================================

@patch("ngRadar_Website.views.views.render")
@patch("ngRadar_Website.views.views.get_obs_events")
def test_status_partial(mock_obs_event, mock_render):
    request = MagicMock()
    
    response = HttpResponse("fake_response")
    mock_render.return_value = response
    mock_obs_event.return_value = "fake_obs_events"

    output = status_partial(request)

    assert output == response
    mock_obs_event.assert_called_once_with()
    mock_render.assert_called_once_with(request, "ngRadar_Website/partials/status_partial.html", mock_obs_event())


# ==============================================================================
# 12. dsoc_event_partial Test
# ==============================================================================

@patch("ngRadar_Website.views.views.render")
@patch("ngRadar_Website.views.views.get_obs_events")
def test_dsoc_event_partial(mock_obs_event, mock_render):
    request = MagicMock()
        
    response = HttpResponse("fake_response")
    mock_render.return_value = response
    mock_obs_event.return_value = "fake_obs_events"
    output = dsoc_event_partial(request)

    assert output == response
    mock_obs_event.assert_called_once_with()
    mock_render.assert_called_once_with(request, "ngRadar_Website/partials/dsoc_home_partial.html", mock_obs_event())


# ==============================================================================
# 13. gbt_event_partial Test
# ==============================================================================

@patch("ngRadar_Website.views.views.render")
@patch("ngRadar_Website.views.views.get_obs_events")
def test_gbt_event_partial(mock_obs_event, mock_render):
    request = MagicMock()
        
    response = HttpResponse("fake_response")
    mock_render.return_value = response
    mock_obs_event.return_value = "fake_obs_events"

    output = gbt_event_partial(request)

    assert output == response
    mock_obs_event.assert_called_once_with()
    mock_render.assert_called_once_with(request, "ngRadar_Website/partials/gbt_home_partial.html", mock_obs_event())