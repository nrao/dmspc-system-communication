from django.urls import path
from .views import views

urlpatterns = [
    # Have not tested if this works as expected
    path('dashboard/', views.dashboard_view, name='dashboard_home'),
    
    # Have not tested if this works as expected
    path('dashboard/update/', views.event_table_partial, name='event_table_update'),
]
