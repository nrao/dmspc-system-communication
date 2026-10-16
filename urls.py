from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from ngRadar_Website.views import views
from django.contrib.auth.decorators import login_not_required
from django_prometheus.exports import ExportToDjangoView



@login_not_required # needed so Prometheus can directly access /metrics endpoint without being redirected from the login page
def metrics(request):
    return ExportToDjangoView(request)


urlpatterns = [
    path("metrics", metrics, name="metrics"),
    path('', views.login_view), 
    path('login/', views.login_view, name='login'),
    path('admin/', admin.site.urls),
    path('', include('ngRadar_Website.urls')),

    # built-in auth views: login, logout, password change/reset, etc.
    # provides url names 'login' and 'logout' and use templates under registration 
    # path('accounts/', include('django.contrib.auth.urls')),
]


# FOR PROTOTYPE ONLY: Allows browser to access the local /media/ folder images
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

