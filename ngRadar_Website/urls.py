from django.urls import path
from .views import views

urlpatterns = [
    path('dashboard/', views.dashboard_view, name='dashboard_home'),
    path('dashboard/update/', views.event_table_partial, name='event_table_update'),
    path('dashboard/image/<int:event_id>/', views.serve_image, name ='serve_image'),
    path("dashboard/observation/", views.toggle_observation, name="toggle_observation"),
]
