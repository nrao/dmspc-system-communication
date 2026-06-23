# Create your views here.
from django.shortcuts import render

def index(request):
        # Renders the template located at ngRadar_Website/templates/index.html
    return render(request, 'ngRadar_Website/index.html')
