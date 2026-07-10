from django.urls import path
from .views import views
from django.contrib.auth.views import LogoutView

urlpatterns = [
    # Home page URLs
    path('home/', views.home_view, name='home'),
    path('home/gbtevent', views.gbt_event_partial, name='gbt_events'),
    path('home/dsocevent', views.dsoc_event_partial, name='dsoc_events'),
    path('submit-waveform/', views.submit_waveform, name='submit_waveform'),

    # Dashboard page URLs
    path('dashboard/', views.dashboard_view, name='dashboard_home'),
    path('dashboard/updates', views.event_table_partial, name='event_table_update'),
    path('dashboard/latency', views.latency_graphing, name='latency_graphing'),
    path('dashboard/image', views.serve_image, name ='serve_image'),


    # Keep as placeholder when we develop this
    # # path to blank page where we will allow new observations to be created
    # path('new_observation/',views.create_observation, name='create_new_observation'),
    
    # add logout path 
    path('logout/', LogoutView.as_view(), name='logout')
]
