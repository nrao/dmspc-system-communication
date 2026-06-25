from django.db import models
from ngRadar_Website.enums import Stations
from argon2 import PasswordHasher

ph = PasswordHasher()  # Initialize the PasswordHasher instance


class User(models.Model):
    uuid = models.UUIDField(primary_key=True, editable=False)
    username = models.CharField(max_length=150)
    password = models.CharField(max_length=128)

    # Hash the password before saving it to the database
    def set_password(self, raw_password):
        self.password = ph.hash(raw_password)
    
    # Check if the provided password matches the hashed password stored in the database
    def check_password(self, raw_password):
        try:
            return ph.verify(self.password, raw_password)
        except Exception:
            return False


class ObservatoryEvent(models.Model):
    # Mapping to your payload variables
    obs_id = models.CharField(max_length=100)
    target = models.CharField(max_length=100)
    product_type = models.CharField(max_length=50)
    product_id = models.CharField(max_length=100, unique=True)
    station = models.PositiveSmallIntegerField(
            choices=Stations.choices,
            default=Stations.GBT
        )    
    creation_time = models.DateTimeField()
    event_time = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)

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

    image_file = models.ImageField(upload_to='ddm_payloads/', blank=True, null=True)
    num_bytes = models.IntegerField(default=0)
    latency_ms = models.FloatField(default=0.0)

    def __str__(self):
        return f"Obs: {self.obs_id} | {self.xmit_station_id} -> {self.rcvr_station_id}"


# what other tables may we need?