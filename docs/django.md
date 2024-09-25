# Django Installation

1. There needs to be an environment variable CFS_DBDIR which points to a location where we can put the databse and the migrations directories. In development you can safely point this to the data directory, but in deployment it should be dealt with by the initial configuration (details to follow).
2. To get going we need the usual django setup stuff:

    ```bash
    python manage.py makemigrations
    python manage.py makemigrations core
    python migrate
    ```

3. This documentation can be be built to be visible on at /site. (We should probably change that location) with

    ```bash
   python manage.py build_docs
    ```
