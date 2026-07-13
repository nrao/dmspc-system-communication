from django.urls import path
from .views import views
from django.contrib.auth.views import LogoutView

urlpatterns = [
    # Home page URLs
    path('home/', views.home_view, name='home'),
    path('home/status', views.status_partial, name="status"),
    path('home/gbtevent', views.gbt_event_partial, name='gbt_events'),
    path('home/dsocevent', views.dsoc_event_partial, name='dsoc_events'),
    path('home/submit-waveform/', views.submit_waveform, name='submit_waveform'),

    # Dashboard page URLs
    path('dashboard/', views.dashboard_view, name='dashboard_home'),
    path('dashboard/updates', views.event_table_partial, name='event_table_update'),
    path('dashboard/latency', views.latency_graphing, name='latency_graphing'),
    path("image/<uuid:uuid>/", views.serve_image, name="serve_image"),

    
    # logout path 
    path('logout/', LogoutView.as_view(), name='logout')
]
