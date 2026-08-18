from pathlib import Path
from dotenv import load_dotenv
from unittest.mock import patch, MagicMock

from ngRadar_Website.views.views import get_obs_events
from ngRadar_Website.enums import Stations
from datetime import datetime, timezone
from ngRadar_Website.enums import Stations
from ngRadar_Website.models.models import gbtEvent, dsocEvent, ObservatoryEvent, uiEvent
from django.test import RequestFactory
from django.http import HttpResponse, HttpResponseRedirect

import uuid
import json

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
        "WEED_S3_INTERNAL_DOMAIN": "seaweedfs.fake.com",
        "WEED_S3_ACCESS_KEY": "fake_access_key",
        "WEED_S3_SECRET_KEY": "fake_key",
        "WEED_S3_BUCKET": "fake_bucket",
        "WEED_S3_PUBLIC_DOMAIN": "fake_domain"
    },
)
@patch("ngRadar_Website.views.views.get_object_or_404")
@patch("ngRadar_Website.views.views.boto3")
@patch("ngRadar_Website.views.views.Config")
def test_serve_image(mock_Config, mock_boto3, mock_get_obj):

    mock_event = MagicMock()
    mock_event.image_key = "images/test.png"
    mock_get_obj.return_value = mock_event

    mock_config = MagicMock()
    mock_Config.return_value = mock_config

    #creating the fake boto3.client:
    mock_s3 = MagicMock()
    mock_boto3.client.return_value = mock_s3

    #set the internal url using our fake WEED_S3_INTERNAL_DOMAIN:
    internal_url = (
        "http://seaweedfs.fake.com/fake_bucket/images/test.png"
    )
    mock_s3.generate_presigned_url.return_value = internal_url

    #call the function:
    output = serve_image(request = "request", uuid = "uuid")

    #assert that the WEED_S3_PUBLIC_DOMAIN variable was used to update the url:
    assert output.url == "http://fake_domain/fake_bucket/images/test.png"
    mock_boto3.client.assert_called_once_with(
            "s3",
            endpoint_url="seaweedfs.fake.com",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_key",
            config = mock_config
        ) #checking that an S3 client was created


# ==============================================================================
# 3. Submit waveform test
# ==============================================================================
@patch("ngRadar_Website.views.views.uuid.uuid4")
@patch("ngRadar_Website.views.views.datetime")
@patch("ngRadar_Website.views.views.waveform_producer")
@patch("ngRadar_Website.views.views.cache")
@patch("ngRadar_Website.views.views.write_transfer_progress")
@patch("ngRadar_Website.views.views.ngrok_endpoint.objects.last")
@patch("ngRadar_Website.views.views.uiEvent.objects.create")
def test_submit_waveform(Mock_UI_EVENT, Mock_bootstrap, Mock_ProgressBar, Mock_Cache, Mock_Producer, Mock_datetime, Test_uuid):
    #create simulated data
    mock_uuid = uuid.UUID('12345678-1234-5678-1234-567812345678')
    test_timestamp = datetime(2026, 8, 17, 12, 30, 45, tzinfo=timezone.utc)
    test_waveform = '45'
    mock_endpoint = "test123endpoint"

    #create fixed return values for UUID and date time
    Test_uuid.return_value = mock_uuid
    Mock_datetime.now.return_value=Mock_datetime

    #generate a mock post request
    factory = RequestFactory()
    myRequest = factory.post('home/submit-waveform/', data={'waveform':test_waveform})

    #mock the bootsrap value
    mock_ngrok = MagicMock()
    mock_ngrok.bootstrap = mock_endpoint
    Mock_bootstrap.return_value = mock_ngrok

    #mock a UI Event
    Mock_EVENT = MagicMock()
    Mock_EVENT.uuid = mock_uuid
    Mock_EVENT.selected_waveform = test_waveform
    Mock_EVENT.event_time = test_timestamp
    Mock_UI_EVENT.return_value = Mock_EVENT

    result = submit_waveform(myRequest)
    
    # Assert MockUiEvent was called
    Mock_UI_EVENT.assert_called_once()

    # Assert waveform_producer was called
    Mock_Producer.assert_called_once()
    
    #get the parameters from the Mock_producer
    waveform_producer_topic = Mock_Producer.call_args[0][0]
    waveform_producer_config = Mock_Producer.call_args[0][1]
    waveform_producer_uuid = Mock_Producer.call_args[0][2]
    waveform_producer_value = Mock_Producer.call_args[0][3]

    #test that data sent in the fake message matches the simulated data
    assert waveform_producer_topic == "user_input"

    assert waveform_producer_config['bootstrap.servers'] == mock_endpoint
    assert waveform_producer_config['client.id'] == 'ui-producer'

    #assert the UUID and convert to hexadecimal
    assert waveform_producer_uuid == mock_uuid.hex

    assert json.loads(waveform_producer_value.decode('utf-8')) == "User input a new waveform."

    # Assert cache was set
    Mock_Cache.set.assert_called_once()

    #assert call to reset progress bar was made
    Mock_ProgressBar.assert_called_once()

# ==============================================================================
# 4. login_view Test
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