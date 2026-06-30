from django.shortcuts import redirect, render
from django.contrib.auth.decorators import login_required

#libraries used for data streaming
import json
from django.http import StreamingHttpResponse, HttpResponse, Http404

from ngRadar_Website.models.models import ObservatoryEvent
from django.contrib import messages
from django.contrib.auth import authenticate, login
from django.db.models import Avg

def login_view(request):
    if request.method == 'POST':
        username_input = request.POST['username']
        password_input = request.POST['password']
        
        # This automatically uses the Argon2 settings to verify the password string
        user = authenticate(request, username=username_input, password=password_input)
        
        if user is not None:
            login(request, user)
            return redirect('dashboard_home')
        else:
            messages.error(request, "Invalid username or password.")
            return render(request, 'registration/login.html')
            
    return render(request, 'registration/login.html')


def get_dashboard_context():
    """Helper function to keep data uniform across view updates"""
    latest_events = ObservatoryEvent.objects.all().order_by('-event_time')[:20]
    
    # Calculate the average latency of the last 20 records in Postgres
    avg_latency = latest_events.aggregate(Avg('latency_ms'))['latency_ms__avg'] or 0.0
    #print(avg_latency)
    # Calculate anything else we need for the initial load of the dashboard
    
    return {
        'events': latest_events,
        'avg_latency': round(avg_latency, 2)
    }


def get_Message_Latency():
    print("Streaming message latency data to frontend...")
    last_message_latency = ObservatoryEvent.objects.last().latency_ms
    last_message_time = ObservatoryEvent.objects.last().event_time
    print(last_message_latency, last_message_time)
    data_to_send = {
        "latency": last_message_latency,
        "time_sent": last_message_time
    }
    yield f"data: {json.dumps(data_to_send)}\n\n"

def latency_graphing(request):
    response = StreamingHttpResponse(
        get_Message_Latency(),
        content_type="text/event-stream; charset=utf-8"
    )
    response["Cache-Control"] = "no-cache"
    return response

@login_required
def dashboard_view(request):
    # this is the initial view to load the dashboard
    context = get_dashboard_context()

    return render(request, 'ngRadar_Website/dashboard.html', context) # pass any other vars to frontend here

def event_table_partial(request):
    # this is the partial template view for updating the observatory events table
    latency_graphing(request)  # Call the function to get the latest message latency
    context = get_dashboard_context()
    return render(request, 'ngRadar_Website/partials/dashboard_updates.html')

def serve_image(request, event_id):

    # get draw bytes from DB 
    raw = ObservatoryEvent.objects.filter(id=event_id).values_list('image_file', flat=True).first()

    # if no image found, show 404
    if not raw: 
        raise Http404
    
    # send raw bytes to browser, labeled as PNG 
    return HttpResponse(bytes(raw), content_type ='image/png')

# Need a function AND another partial template for handling the user inputted payload
# Don't worry about this until Sprint 45 I think
# def user_input_partial(request):
#     # this is the partial template view for handling user input
#     # can we store user inputted data to the database here? should we?
#     # how should we handle sending the user inputted payload to the Kafka topic?
#     return render(request, 'user_input.html') # just an example .html, have not actually created this


