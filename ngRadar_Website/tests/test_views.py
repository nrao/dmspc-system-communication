from pathlib import Path

from dotenv import load_dotenv
from unittest.mock import patch, MagicMock

from ngRadar_Website.views.views import get_obs_events
from ngRadar_Website.enums import Stations
from datetime import datetime, timezone, timedelta
import pytest
from ngRadar_Website.enums import Stations
from ngRadar_Website.models.models import gbtEvent, dsocEvent, ObservatoryEvent, uiEvent
#from ngRadar_Website.views.views import get_obs_events
from django.test import RequestFactory
from django.http import HttpResponse, HttpResponseRedirect

import random,string

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


# ==============================================================================
# 3. login_view Test
# ==============================================================================


@patch("ngRadar_Website.views.views.logout_view")
def test_login_view_auth(mock_logout):
    """Scenario 1: user is authenticated"""

    #We need this django function to generate a fake http request for us:
    factory = RequestFactory()
    request = factory.get("/login/")

    #the following authentication is True to satisfy the if statement:
    request.user = MagicMock()
    request.user.is_authenticated = True

    #We need the formatting to be in HttpResponse to satisfy the cache_control decorator:
    mock_response = HttpResponse("logged out")
    mock_logout.return_value = mock_response

    #Call the function:
    output = login_view(request)

    assert output == mock_response
    mock_logout.assert_called_once_with(request)


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