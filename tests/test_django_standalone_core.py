from core.db.standalone import setup_django
from pathlib import Path
import pytest


def test_blank_database(tmp_path):
    """ Test that the system can install a blank database with our basic model"""

    dbfile = str(Path(tmp_path)/'test.db')
    migrations_location = str(Path(tmp_path)/'migrations')

    print('db',dbfile, migrations_location)
    setup_django(db_file=dbfile,
                 migrations_location=migrations_location)


    from core.db.models import File

    file1 = File(name='myfile1',path='/tmp/myfile1',size=1)
    file2 = File(name='myfile2',path='/tmp/myfile2',size=2)
    file1.save()
    file2.save()

    files = File.objects.all()
    assert len(files) == 2
    
