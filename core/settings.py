from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


# Use BigAutoField for Default Primary Key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': Path.home()/'.cfstore/cfsdb.sqlite3',
            }
        }

INSTALLED_APPS=('db',)

TIME_ZONE='UTC'
USE_TZ=True

"""
To connect to an existing postgres database, first:
pip install psycopg2
then overwrite the settings above with:

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'YOURDB',
        'USER': 'postgres',
        'PASSWORD': 'password',
        'HOST': 'localhost',
        'PORT': '',
    }
}
"""