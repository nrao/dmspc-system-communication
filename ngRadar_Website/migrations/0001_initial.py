from django.db import migrations
from django.contrib.auth.models import User


# NOTE: Delete this migration after we get a auth_user table in the database, this is just a temporary workaround to create a dev user account for testing purposes.

def create_dev_accounts(apps, schema_editor):
    # This automatically builds persistent team accounts during migration
    if not User.objects.filter(username="dev").exists():
        User.objects.create_superuser("dev", "dev1@company.com", "SecurePassword123")
        
    

def remove_dev_accounts(apps, schema_editor):
    # Reverses the operation if you ever roll back the migration
    User.objects.filter(username__in=["dev"]).delete()

class Migration(migrations.Migration):

    dependencies = [
        
    ]

    operations = [
        migrations.RunPython(create_dev_accounts, reverse_code=remove_dev_accounts),
    ]
