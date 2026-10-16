from django.urls import path
from .views import views
from django.contrib.auth.views import LogoutView


urlpatterns = [
    # Home page URLs
    path('home/', views.home_view, name='home'),
    path('home/submit-waveform/', views.submit_waveform, name='submit_waveform'),
    path('home/lock-status/', views.lock_status, name='lock_status'),
    path("home/image/<uuid:uuid>/", views.serve_image, name="serve_image"),

    # Dashboard page URLs
    path('dashboard/', views.dashboard_view, name='dashboard_home'),
    path('dashboard/updates/', views.event_table_partial, name='event_table_update'),
    path("dashboard/latency-data/", views.latency_data, name="latency_data"),

    # SSE (Server-Sent Events) URLs:
    path("events/stream/", views.sse_stream, name="sse_stream"),
    
    # Authentication paths 
    path('logout/', views.logout_view, name='logout')
]
