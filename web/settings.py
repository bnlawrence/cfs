"""
Django settings for web project.

Generated by 'django-admin startproject' using Django 5.1.1.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/5.1/ref/settings/
"""
from pathlib import Path
import os, sys



# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-=-4nv-lhjq2bk3ovfmk)8yy%-glw5wx2x5j5gj#3-x&z&wg75='

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'cfs',
    'web',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'web.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'web.wsgi.application'

# We have to do some careful stuff around migrations and databases
# so users can have their own version in sensible places, as well
# as putting the project data outside the python path of the project.

# First, let's sort out where we are going to put our database and migrations directory
DBDIR = os.getenv('CFS_DBDIR', None)
if DBDIR is None: 
    raise ValueError(
        'Environment variable CFS_DBDIR must point to a directory for the DB and migrations')

DBDIR = Path(DBDIR)
if not DBDIR.is_dir():
    raise ValueError ('Environment variable CFS_DBDIR does not point to an existing directory')

# Now we need a migrations directory for our application
# we need to be able to make this directory importable to be used for 
# 
# migrations
if str(DBDIR) not in sys.path:
    sys.path.append(DBDIR)

# db this is the name of the database in the app label meta of the models in 
# cfs.models.py. It needs to be at least one directory away from the migrations dir
# to avoid import conflicts with the main cfs directory.
migrations_dir = Path(DBDIR)/ 'cfs_migrations'
if not migrations_dir.exists():
    migrations_dir.mkdir(parents=True, exist_ok=True)

# it needs to be a valid importable module
init_file = migrations_dir/'__init__.py'
init_file.touch(exist_ok=True)

dblast = Path(DBDIR).name

# now force everthing to use that
MIGRATION_MODULES={
    'cfs': f'{dblast}.cfs_migrations',
}


# Database
# https://docs.djangoproject.com/en/5.1/ref/settings/#databases


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': Path(DBDIR) / 'cfsdb.sqlite3',
    }
}


# Password validation
# https://docs.djangoproject.com/en/5.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.1/howto/static-files/

STATIC_URL = 'static/'

# Default primary key field type
# https://docs.djangoproject.com/en/5.1/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
