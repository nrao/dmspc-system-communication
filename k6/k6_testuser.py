"""
DO NOT run this script directly. Create a test user first:
In your terminal:
    docker compose exec ngradar_website python manage.py shell -c \
    'from ngRadar_Website.models.models import ObservatoryEvent; print(ObservatoryEvent.objects.exclude(image_key__isnull=True).exclude(image_key="").order_by("-event_time").first().uuid)'
That will output an image uuid, and you need to define your K6_IMAGE_ID var in your .env with that value.
Be sure K6_USERNAME and K6_PASSWORD are also defined in your .env
Still within the shell, enter the python shell by running python manage.py shell
Then you can paste the script below into the shell. Upon pasting, the user will be created.
Now you can run the k6 tests that require authentication, they will use this test user.

Now try running ./control.sh smoketest after exiting all shells
"""

from django.contrib.auth import get_user_model

User = get_user_model()

user = User.objects.create_user(
    username="k6-testuser",
    email="k6-test@example.com",
    password="k6-password",
)

print("User has been created.")
print(f"User permissions for {user.username} are:") # False False True
print(f"Staff: {user.is_staff}",
     f"Superuser: {user.is_superuser}", 
     f"Active: {user.is_active}",
     ) 
