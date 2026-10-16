"""
ASGI config for prototype_project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/6.0/howto/deployment/asgi/
"""

import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')

from django.core.asgi import get_asgi_application
django_application = get_asgi_application()

from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware
from ngRadar_Website.views.views import app as fastapi_app

#create an asynchronous sever which works with both Django and FastAPI
#aync_app = WSGIMiddleware(django_application)
aync_app.mount("/", WSGIMiddleware(django_application))
async_app.mount("/api", fastapi_app)
