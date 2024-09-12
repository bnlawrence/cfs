import cf
import pytest
from pathlib import Path

from core.db.standalone import setup_django

VARIABLE_LIST = ['air_temperature','specific_humidity']

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
def django_dependencies():
    """ 
    The database (and it's contents) is used in all the following
    tests, and is progressively modified as the tests proceed.
    Posix imports some django dependent stuff as well. 
    """
    from core.db.interface import CollectionDB
    from core.plugins.posix import Posix
    db = CollectionDB()
    return db, Posix(db,'vftesting')

@pytest.fixture
def posix_info(tmp_path, inputfield):
    posix_path = tmp_path / 'posix_root'  
    posix_path.mkdir(exist_ok=True)  
    
    sz = 0 
    filenames = [posix_path/'test_file1.nc', posix_path/'test_file2.nc']
    for v,f in zip(VARIABLE_LIST,filenames):
        inputfield.standard_name = v
        cf.write([inputfield,],f)
        sz += f.stat().st_size

    return posix_path,sz


def test_cf_reading(posix_info):
    """ 
    This test just makes sure we can read the input test data
    If this fails, this test file is borked.
    """
    posix_path, s = posix_info
    files = Path(posix_path).glob('*.nc')
    for f in files:
        flds = cf.read(f)
    
def test_posix_class_basic(django_dependencies, posix_info):
    """ 
    Test uploading a couple of files without a hierarchy and
    nested subcollections
    """
    posix_path, s = posix_info
    test_db, p = django_dependencies
    p.add_collection(
        str(posix_path),
        'posix_test_collection',
        'collection of variables from the test data')
    cset = test_db.collections_retrieve()
    c1 = cset[0]
    assert c1.volume == s
    assert set([x.standard_name.value for x in c1.variables.all()]) == set(VARIABLE_LIST)

def test_posix_class_nested():
    raise NotImplementedError