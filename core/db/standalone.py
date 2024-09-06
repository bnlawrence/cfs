from pathlib import Path
import django
from django.conf import settings
from django.db import connection
from django.core.management import execute_from_command_line
import sys
import warnings

def check_and_create_database():
    """ 
    This checks to see if the database file exists, and if it includes at least one of 
    our tables.  If not, it creates the database, and reads our models.py using
    makemigrations which is a set of instructions as to how to make the tables,
    before using migrations to make the tables. 

    #FIXME: Currently, this DOES not update the database schema if an an older version database
    is found (and that is not checked for).
    """
    # Check if the tables already exist by checking if one of the tables in your models exists
    with warnings.catch_warnings():
        # suppressing warning about discouraging access to database during app initialisation
        warnings.simplefilter('ignore')
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=%s", ['db_tag'])
            result = cursor.fetchone()
    # If the table doesn't exist, run the migration commands to create the tables
    if not result:
        print("Database doesn't exist or tables are missing. Creating tables...")
        # Apply migrations programmatically to create the tables
        execute_from_command_line(['manage.py','makemigrations','db'])
        execute_from_command_line(['manage.py','migrate'])
    else:
        print("Using existing database without modification")

def setup_migrations_location(migrations_location):
    """
    This is a complex hack, which need because we want to ensure we can have a 
    shared package, but individuals have their own databases.
    
    django needs to be able to find an importable module at the migrations location, 
    which means the location has to exist and be in the system path.  The "module"
    needs to have our app name (db).

    :param migrations_location: The root location for the migrations directory.
    :return: A dictionary suitable for Django's MIGRATION_MODULES setting.
    """
    migrations_dir = Path(migrations_location) / 'db'
    
    # Ensure the migrations directory exists
    if not migrations_dir.exists():
        migrations_dir.mkdir(parents=True, exist_ok=True)
    
    # Add the parent directory to sys.path so that it can be importable
    sys.path.append(migrations_location)
    
    # Create __init__.py to make it a valid Python module
    init_file = migrations_dir / '__init__.py'
    init_file.touch(exist_ok=True)

    # Debug output to ensure the directory and __init__.py were created
    # print('Init exists:', init_file.name, init_file.exists())

    # Return a dictionary for the MIGRATION_MODULES setting
    return {'db': 'db'}


def setup_django(db_file=None,
                 migrations_location={}):
    """
    Used to setup the django settings from within our code, rather than
    having it as a file - this way we can change settings dynamically.
    :param db_file: Name of the actual database file (including full path).
    :param migrations_location: The root location for the migrations directory.
    """
    # Configure settings for standalone use
    if db_file is None:
        db_file =   Path.home()/'.cfstore/cfsdb.sqlite3'

    # if we want to use a non-standard location (i.e. not in web app,
    # we need to use this.)
    if migrations_location:
        migrations_location = setup_migrations_location(migrations_location)

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
        MIGRATION_MODULES=migrations_location
    )

    check_and_create_database()
    django.setup()
  