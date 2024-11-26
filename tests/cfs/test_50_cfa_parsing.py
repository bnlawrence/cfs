from pathlib import Path
from cfs.db.cfa_tools import CFAhandler
from cfs.db.cfparsing import parse_fields_todict
from cfs.db.standalone import setup_django


import cf
import numpy as np
import pytest

@pytest.fixture(scope="module", autouse=True)
def setup_test_db(tmp_path_factory, request):
    """ 
    Get ourselves a db to work with. Note that this database is progressively
    modified by all the tests that follow. So if you're debugging tests, you 
    have to work though them consecutively. 
    """
    module_name = request.module.__name__  # Get the module (test file) name
    tmp_path = tmp_path_factory.mktemp(module_name)  # Create a unique temp directory for the module
    dbfile = str(Path(tmp_path) / f'{module_name}.db')
    migrations_location = str(Path(tmp_path)/'migrations')
    setup_django(db_file=dbfile,  migrations_location=migrations_location)
    yield # This marks the end of the setup phase and begins the test execution

@pytest.fixture
def test_db():
    """ 
    This database (and it's contents) is used in all the following
    tests, and is progressively modified as the tests proceed.
    """
    from cfs.db.interface import CollectionDB
    return CollectionDB()


@pytest.fixture
def cfa_only(tmp_path, inputfield):
    """
    Make three copies of our basic netcdf file, and then aggregate it
    """

    posix_path = tmp_path / 'posix_root'  
    posix_path.mkdir(exist_ok=True)  

    print('CFA setup with three files and one field')
    filenames = [posix_path/f'test_file{x}.nc' for x in range(3)]
    print(inputfield)
    for f, index in zip(filenames, range(0, 36, 12)):
        cf.write(inputfield[index:index+12], f)

    f = cf.read(posix_path.glob('*.nc'), cfa_write='field')[0]

    cfa_file = str(posix_path/'test_file.cfa')
    cf.write(f, cfa_file, cfa='field')

    # Check that the cfa file reads alright
    g = cf.read(cfa_file, cfa_write='field')

    return posix_path

def test_cfa_handler(cfa_only):

    fields = cf.read(cfa_only.glob('*.cfa'))

    c = CFAhandler(expected_fields=len(fields))

    for f in fields:
        c.parse_field_to_manifest(f)

    assert len(c.known_manifests) == 1

def test_variable_manifest_linkage(cfa_only):
    """ 
    The manikey should link variables to manifests found in the cfa file
    """
    fields = cf.read(cfa_only.glob('*.cfa'))

    descriptions, manifests = parse_fields_todict(fields[0:10], cfa=True)

    vkey = descriptions[0].pop('manikey')
    mkey = list(manifests.keys())[0]
    assert vkey == mkey

def test_manifest_to_db(cfa_only, test_db):
    """ test uploading a manifest to the database"""

    fields = cf.read(cfa_only.glob('*.cfa'))
    c = CFAhandler(expected_fields=len(fields))
    for f in fields:
        key = c.parse_field_to_manifest(f)
    manifest = c.known_manifests[key]
    manifest.pop('manikey')
    #make a cfa file 
    prop1 = {'name':'manicfa','path':'/tmp/cfatest','size':10,'type':'A','location':'init'}
    f1 = test_db.file.create(prop1)
    manifest['cfa_file'] = f1
    manifest= test_db.manifest.add(manifest)
    

def test_remove_manifests(test_db):
    """ Should also get rid of the fragments"""
    manifests = test_db.manifest.all()
    for m in manifests:
        m.delete()
    files = test_db.file.all()
    assert len(files) == 1
    for f in files:
        f.delete()


    






    

