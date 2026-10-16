#libraries to get files from the outside directory
from django.db import models
import uuid
from ngRadar_Website.enums import Stations, Status



class ObservatoryEvent(models.Model):
    # History table for all historical events that occur during an observation. 
    # This includes events from GBT, VLBA, DSOC, and UI events.
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    gbt_uuid = models.UUIDField(
        blank=True,
        null=True,
        db_index=True,
    )
    object_id = models.CharField(
        max_length=100,
        blank=True,
        null=True,
    )
    target = models.CharField(
        max_length=100,
        blank=True,
        null=True,
    )
    waveform_requester = models.CharField(max_length=100, blank=True, null=True)
    tx_waveform = models.CharField(max_length=100, blank=True, null=True)
    rec_waveform = models.CharField(max_length=100, blank=True, null=True)
    product_type = models.CharField(max_length=50, blank=True, null=True)
    product_id = models.CharField(max_length=100, blank=True, null=True)
    station = models.PositiveSmallIntegerField(
            choices=Stations.choices,
            default=Stations.GBT, blank=True, null=True
        )    
    event_time = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True,) 
    xmit_station = models.IntegerField(
        choices=Stations.choices,
        blank=True,
        null=True,
        )
    rcvr_station = models.IntegerField(
        choices=Stations.choices,
        blank=True,
        null=True,
    )
    image_key = models.CharField(max_length=500, blank=True, null=True)
    num_bytes = models.IntegerField(blank=True, null=True)
    latency_ms = models.FloatField(default=0.0)
    transfer_uuid = models.UUIDField(
        blank=True,
        null=True,
        db_index=True,
    )
    status = models.PositiveSmallIntegerField(
        choices=Status.choices,
        blank=True,
        null=True,
    )
    message = models.TextField(blank=True, null=True, default="")


    def __str__(self):
        return f"Obs: {self.object_id} | {self.xmit_station} -> {self.rcvr_station}"


