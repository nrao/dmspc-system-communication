from django.contrib import admin
from django.urls import path, include
from django.contrib.auth import views as auth_views 
from django.views.generic.base import RedirectView, TemplateView
from django.conf import settings
from django.conf.urls.static import static




urlpatterns = [
    # Need to verify these urls all work as expected after all the restructuring
    path('', RedirectView.as_view(url='/login/', permanent=False)),


    # this is the current login view - a Django built in "LoginView", but should we create a custom login view?
    # that way when we store user credentials in db, we can have more control over the authentication process?
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),

    path('ngRadar_Website/', TemplateView.as_view(template_name='ngRadar_Website/index.html'), name='home'),
    path('admin/', admin.site.urls),
]


# FOR PROTOTYPE ONLY: Allows browser to access the local /media/ folder images
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
