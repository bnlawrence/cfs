#!/bin/bash

# Check if CFS_DBDIR is set
if [ -z "$CFS_DBDIR" ]; then
    echo "Error: Environment variable CFS_DBDIR is not set."
    exit 1
fi

# Check if CFS_DBDIR points to a valid directory
if [ ! -d "$CFS_DBDIR" ]; then
    echo "Error: CFS_DBDIR does not point to a valid directory."
    exit 1
fi

rm -r -f $CFS_DBDIR/cfs_migrations $CFS_DBDIR/cfsdb.sqlite3
python manage.py makemigrations cfs
python manage.py migrate
DJANGO_SUPERUSER_USERNAME=admin DJANGO_SUPERUSER_EMAIL=admin@example.com DJANGO_SUPERUSER_PASSWORD=admin ./manage.py createsuperuser --noinput