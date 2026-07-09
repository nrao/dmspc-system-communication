#libraries to get files from the outside directory
import sys
from pathlib import Path
from django.db import models
import uuid
from ngRadar_Website.enums import Stations



class ObservatoryEvent(models.Model):
    # History table for all events from both gbtEvent and dsocEvent tables
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    object_id = models.CharField(max_length=100)
    target = models.CharField(max_length=100)
    tx_waveform = models.CharField(max_length=100, blank=True, null=True)
    rec_waveform = models.CharField(max_length=100, blank=True, null=True)

    product_type = models.CharField(max_length=50, blank=True, null=True)
    product_id = models.CharField(max_length=100, blank=True, null=True)
    station = models.PositiveSmallIntegerField(
            choices=Stations.choices,
            default=Stations.GBT, blank=True, null=True
        )    
    event_time = models.DateTimeField()
    created_at = models.DateTimeField(blank=True, null=True) 

    # This allows us to track the transmitter and receiver stations for each event
    xmit_station = models.CharField(
        max_length=100, 
        choices=Stations.choices, 
        blank=True, null=True
    )
    rcvr_station = models.CharField(
        max_length=100, 
        choices=Stations.choices, 
        blank=True, null=True
    )
    
    image_key = models.CharField(max_length=500, blank=True, null=True)
    num_bytes = models.IntegerField(blank=True, null=True)
    latency_ms = models.FloatField(default=0.0)


    def __str__(self):
        return f"Obs: {self.object_id} | {self.xmit_station} -> {self.rcvr_station}"


class gbtEvent(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    object_id = models.CharField(max_length=100)
    target = models.CharField(max_length=100)
    tx_waveform = models.CharField(max_length=100)
    rec_waveform = models.CharField(max_length=100)
    event_time = models.DateTimeField()
    latency_ms = models.FloatField(default=0.0)

    def __str__(self):
        return f"GBT Event: {self.object_id} | {self.event_time}"


class dsocEvent(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    object_id = models.CharField(max_length=100)
    target = models.CharField(max_length=100)
    image_key = models.CharField(max_length=500, blank=True, null=True)
    num_bytes = models.IntegerField()
    event_time = models.DateTimeField()
    latency_ms = models.FloatField(default=0.0)

    def __str__(self):
        return f"DSOC Event: {self.object_id} | {self.event_time}"

    

class uiEvent(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    selected_waveform = models.CharField(max_length=100)
    event_time = models.DateTimeField()

    def __str__(self):
        return f"UI Event: {self.selected_waveform} | {self.event_time}"