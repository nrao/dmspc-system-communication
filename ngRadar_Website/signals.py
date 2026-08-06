from django.db.models.signals import post_save
from django.dispatch import receiver
from ngRadar_Website.enums import Stations
from django.utils import timezone
from ngRadar_Website.models.models import (
    gbtEvent,
    dsocEvent,
    ETransferEvent,
    ObservatoryEvent,
)

@receiver(post_save, sender=gbtEvent)
def create_obsevent_from_gbt(sender, instance, created, **kwargs):
    if not created:
        return

    ObservatoryEvent.objects.create(
        object_id=instance.object_id,
        target=instance.target,
        tx_waveform=instance.tx_waveform,   # Included for GBT
        rec_waveform=instance.rec_waveform, # Included for GBT
        image_key=None,                     # GBT records do not have images
        num_bytes=None,                     # GBT records do not have images
        event_time=instance.event_time,
        latency_ms=instance.latency_ms,
        station=Stations.GBT,      
        xmit_station=Stations.GBT, 
        rcvr_station=Stations.HN,
        status=None,
    )


@receiver(post_save, sender=dsocEvent)
def create_obsevent_from_dsoc(sender, instance, created, **kwargs):
    if not created:
        return

    ObservatoryEvent.objects.create(
        object_id=instance.object_id,
        target=instance.target,
        tx_waveform=None,
        rec_waveform=None,
        image_key=instance.image_key,
        num_bytes=instance.num_bytes,
        event_time=instance.event_time,
        latency_ms=instance.latency_ms,
        station=Stations.DSOC,
        xmit_station=instance.xmit_station,
        rcvr_station=instance.rcvr_station,
        transfer_uuid=instance.transfer_uuid,
        status=None,
    )



@receiver(post_save, sender=ETransferEvent)
def create_obsevent_from_etransfer(sender, instance, created, **kwargs):
    if not created:
        return

    ObservatoryEvent.objects.create(
        object_id=instance.object_id,
        target=instance.target,
        tx_waveform=None,
        rec_waveform=None,
        image_key=None,
        num_bytes=instance.num_bytes,
        event_time=instance.event_time,
        latency_ms=instance.latency_ms,
        station=instance.station,
        xmit_station=Stations.GBT,
        rcvr_station=Stations.HN,
        transfer_uuid=instance.transfer_uuid,
        status=instance.status,
    )
