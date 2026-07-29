from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from ngRadar_Website.views.views import submit_waveform
from ngRadar_Website.models.models import uiEvent

import uuid, random

from django.test import RequestFactory

class simulated_uiEvent:
    def create(self,uuid,selected_waveform,event_time):
        self.uuid = uuid
        self.selected_waveform = selected_waveform
        self.event_time = event_time

@patch("ngRadar_Website.views.views.uiEvent")
@patch("ngRadar_Website.views.views.ngrok_endpoint")
def test_waveform_submission(Mock_ngrok_endpoint, Mock_uiEvent):
    #create a simulated post request
    theFactory = RequestFactory()

    #add waveform data
    Mock_waveform = random.randint(45,55)
    payload = {'waveform':Mock_waveform}
    mockRequest = theFactory.post('/home/submit-waveform/', data = payload)

    #create a simulated ui event
    uuid_input = uuid.uuid4()
    date_timeNow = datetime.now(timezone.utc)
    sim_uiEvent1 = simulated_uiEvent(uuid_input,Mock_waveform,date_timeNow)

    Mock_uiEvent.objects = sim_uiEvent1 #insert into the function

    #mock bootstrap
    Mock_ngrok_endpoint.objects.last().bootstrap = "My-SecurE-tunnel!"

    redirection=submit_waveform(mockRequest)

    print (redirection)

