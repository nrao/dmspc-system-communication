from django.apps import AppConfig
import os

class NgradarWebAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "ngRadar_Website"

    def ready(self):
        if (
            os.getenv(
                "ENABLE_UI_KAFKA_CONSUMER",
                "false",
            ).lower()
            != "true"
        ):
            return

        from ngRadar_Website.ui_kafka import start_ui_kafka_consumer

        start_ui_kafka_consumer()
        


