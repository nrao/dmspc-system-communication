from django.apps import AppConfig
from django.db.models.signals import post_save

class NgradarWebAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = 'ngRadar_Website'

    def ready(self):
        from ngRadar_Website.models.models import dsocEvent, gbtEvent, ETransferEvent
        from ngRadar_Website.signals import create_obsevent_from_gbt, create_obsevent_from_dsoc, create_obsevent_from_etransfer
        
        post_save.connect(create_obsevent_from_gbt, sender=gbtEvent, weak=False)
        post_save.connect(create_obsevent_from_dsoc, sender=dsocEvent, weak=False)
        post_save.connect(create_obsevent_from_etransfer, sender=ETransferEvent, weak=False)
