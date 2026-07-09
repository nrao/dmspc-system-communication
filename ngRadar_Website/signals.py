from django.db.models.signals import post_save
from django.dispatch import receiver

from .models.models import (
    gbtEvent,
    dsocEvent,
    ObservatoryEvent,
)
from .enums import Stations


# Event triggered when a new gbtEvent is created
# it will automatically create a corresponding ObservatoryEvent entry
@receiver(post_save, sender=gbtEvent)
def create_observatory_from_gbt(sender, instance, created, **kwargs):
    if not created:
        return

    ObservatoryEvent.objects.create(
        object_id=instance.object_id,
        target=instance.target,
        tx_waveform=instance.tx_waveform,
        rec_waveform=instance.rec_waveform,
        event_time=instance.event_time,
        latency_ms=instance.latency_ms,
        station=Stations.GBT,
    )


# Event triggered when a new dsocEvent is created
# it will automatically create a corresponding ObservatoryEvent entry
@receiver(post_save, sender=dsocEvent)
def create_observatory_from_dsoc(sender, instance, created, **kwargs):
    if not created:
        return

    ObservatoryEvent.objects.create(
        object_id=instance.object_id,
        target=instance.target,
        image_url=instance.image_url,
        num_bytes=instance.num_bytes,
        event_time=instance.event_time,
        latency_ms=instance.latency_ms,
        station=Stations.DSOC,
    )