from pathlib import Path
import django
from django.conf import settings
from django.db import connection
from django.core.management import execute_from_command_line,call_command

def check_and_create_database():
    # Check if the tables already exist by checking if one of the tables in your models exists
    with connection.cursor() as cursor:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=%s", ['core_tag'])
        result = cursor.fetchone()

    print('Checking', result)

    # If the table doesn't exist, run the migration commands to create the tables
    if not result:
        print("Database doesn't exist or tables are missing. Creating tables...")
        # Apply migrations programmatically to create the tables
        execute_from_command_line(['manage.py','makemigrations','db'])
        #call_command('makemigrations', 'db', interactive=False)
        execute_from_command_line(['manage.py','migrate'])
        #call_command('migrate', interactive=False)


    else:
        print("Database exists, skipping table creation.")

def setup_django(db_file=None):
    # Configure settings for standalone use
    if db_file is None:
        db_file =   Path.home()/'.cfstore/cfsdb.sqlite3'
   
    # At this step we override the default settings with our own
    settings.configure(
        BASE_DIR = Path(__file__).resolve().parent.parent,
        DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField',
        INSTALLED_APPS=('core.db',),
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': db_file,
            }
        },
        TIME_ZONE='UTC',
        USE_TZ=True,
    )

    check_and_create_database()
    django.setup()
  