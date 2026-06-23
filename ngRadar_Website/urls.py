from django.urls import path
from .views import views


urlpatterns = [
    # Maps the root URL of this app to the index view function
    path('', views.index, name='index'),
]
