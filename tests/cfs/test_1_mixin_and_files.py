from cfs.db.standalone import setup_django
from pathlib import Path
import pytest


def test_mixin(tmp_path, request):
    """ Test that the basic function of the mixin """

    module_name = request.module.__name__  # Get the module (test file) name
    dbfile = str(Path(tmp_path) / f'{module_name}.db')
    migrations_location = str(Path(tmp_path)/'migrations')

    print('db',dbfile, migrations_location)
    setup_django(db_file=dbfile,
                 migrations_location=migrations_location)

    from cfs.db.interface import CollectionDB


    prop1 = {'name':'myfile1','path':'/tmp/myfile1','size':10,'type':'S','location':'init'}
    prop2 = {'name':'myfile2','path':'/tmp/myfile2','size':10,'type':'F','location':'init'}

    db = CollectionDB()

    f1 = db.file.create(prop1)
    f2 = db.file.create(prop2)

    loc = db.location.retrieve('init')
    assert loc.volume==20

    f3,created = db.file.get_or_create(prop1)
    assert str(f3) == str(f1)
    assert created == False

    files = db.file.all()
    assert len(files) == 2

    file = db.file.retrieve(name='myfile2')
    assert str(file) == str(f2)

    sfiles = db.file.retrieve_all(size=10)
    assert len(sfiles) == 2

    db.file.queryset_delete(files)
    loc = db.location.retrieve('init')
    assert loc.volume == 0

    files = db.file.all()
    assert len(files) == 0

    with pytest.raises(FileNotFoundError):
        db.file.retrieve(name='abc123')

    tags = db.tag.all() 
    with pytest.raises(PermissionError):
        db.file.queryset_delete(tags)
    
    db.location.delete('init')


