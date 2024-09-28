rm -r -f data/cfs_migrations data/cfsdb.sqlite3
python manage.py makemigrations cfs
python manage.py migrate
DJANGO_SUPERUSER_USERNAME=admin DJANGO_SUPERUSER_EMAIL=admin@example.com DJANGO_SUPERUSER_PASSWORD=admin ./manage.py createsuperuser --noinput
time python data/canari.py
