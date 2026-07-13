from dotenv import load_dotenv
from ngRadar_Website.models.models import ngrok_endpoint
from pathlib import Path
from django.core.management.base import BaseCommand



load_dotenv()  # Load environment variables from .env file

p = Path("../../../../out/ngrok_endpoint.env")
text = p.read_text().strip()

bootstrap = None
for line in text.splitlines():
    if line.startswith("BOOTSTRAP_SERVER="):
        bootstrap = f"{line.split('=', 1)[1].strip()}"
        break

if not bootstrap:
    raise RuntimeError("BOOTSTRAP_SERVER not found in /out/ngrok_endpoint.env")



def publish_DB(bootstrap):

    try:
        record = ngrok_endpoint.objects.create(bootstrap=bootstrap)
        print("Endpoint saved to database successfully.")
        return record  # <-- Return the actual object record

    except Exception as e:
        print(f"Database error: {e}")
        return None  # <-- Return None if something broke



class Command(BaseCommand):
    help = "Runs the ngrok-writer worker"

    def handle(self, *args, **options):
        print("Writing ngrok endpoint to DB")
        print(f"Bootstrap writing to DB: {bootstrap}")
        publish_DB(bootstrap)
