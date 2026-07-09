from django.apps import AppConfig


class NgradarWebAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = 'ngRadar_Website'

    def ready(self):
        import ngRadar_Website.signals
