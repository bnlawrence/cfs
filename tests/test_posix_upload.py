import cf
import pytest
from pathlib import Path

from core.db.standalone import setup_django

VARIABLE_LIST = ['air_temperature','specific_humidity','air_potential_temperature']

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
def posix_info(tmp_path, inputfield):
    posix_path = tmp_path / 'posix_root'  
    posix_path.mkdir(exist_ok=True)  
    
    sz = []
    filenames = [posix_path/f'test_file{x}.nc' for x in range(3)]
    for v,f in zip(VARIABLE_LIST,filenames):
        inputfield.standard_name = v
        cf.write([inputfield,],f)
        sz.append(f.stat().st_size)
    return posix_path,sz

@pytest.fixture
def posix_nest(tmp_path, inputfield):
    posix_root = tmp_path / 'posix_root1' 
    posix_down = posix_root/'subset1/subset2' 
    posix_down.mkdir(parents=True, exist_ok=True)  

    directories = ['','subset1/','subset1/subset2/']

    sz = []
    filenames = [posix_root/f'{d}test_file{x}.nc' for x,d in enumerate(directories)]
    #print('Created',[p.relative_to(posix_root) for p in filenames])
    for v,f in zip(VARIABLE_LIST,filenames):
        inputfield.standard_name = v
        cf.write([inputfield,],f)
        sz.append(f.stat().st_size)

    return posix_root,sz
    
def test_posix_class_basic(django_dependencies, posix_info):
    """ 
    Test uploading a couple of files without a hierarchy and
    nested subcollections
    """
    posix_path, s = posix_info
    test_db, p, ignore = django_dependencies
    p.add_collection(
        str(posix_path),
        'posix_test_collection',
        'collection of variables from the test data')
    c1 = test_db.collection_retrieve('posix_test_collection')
    assert c1.volume == sum(s)
    assert set([x.standard_name.value for x in c1.variables.all()]) == set(VARIABLE_LIST)


def test_parents(django_dependencies, posix_nest):
    """ 
    Find the subcollection list for a given directory. This just
    tests the construction of the subcollections
    """
    posix_path, s = posix_nest
    ignore1, ignore2, get_parent_paths = django_dependencies
    expected = [[],['head/subset1'],['head/subset1/subset2', 'head/subset1']]
    got = []
    for p in posix_path.rglob('*.nc'):
        parents = get_parent_paths(p, posix_path, 'head')
        got.append(parents)
    assert expected == got

def test_posix_class_nested(django_dependencies, posix_nest):
    """ 
    Test uploading a couple of files with a hierarchy and
    nested subcollections
    """
    posix_path, s = posix_nest
    test_db, p, ignore = django_dependencies
    p.add_collection(
        str(posix_path),
        'posix_test_collection2',
        'collection of variables from the test data',
        subcollections=True)
    c1 = test_db.collection_retrieve('posix_test_collection2')
    assert set([x.standard_name.value for x in c1.variables.all()]) == set(VARIABLE_LIST)
    assert c1.volume == sum(s)
    assert len(test_db.relationships_retrieve('posix_test_collection2','parent_of')) == 1
    c2 = test_db.collection_retrieve('posix_test_collection2/subset1')
    assert c2.volume == 0  # files only appear in the parent collection
    assert c1.variables.count() == 3
    assert c2.variables.count() == 2
    from core.db.models import Relationship
    rout = test_db.relationships_retrieve('posix_test_collection2').count()
    rin = test_db.relationships_retrieve('posix_test_collection2',outbound=False).count()
    assert rout+rin == 2



def test_deleting_collections(django_dependencies):
    """
    We should be able to empty all those subcollections which have no files
    trivially. 
    """
    test_db, ignore , ignore = django_dependencies

    n_collections = test_db.collections_retrieve().count()
    c = test_db.collection_retrieve('posix_test_collection2')
    removed = test_db.collection_delete_subdirs(c)
    assert removed == 2
    remaining = test_db.collections_retrieve()
    assert remaining.count() == n_collections-2

def test_cleanup(django_dependencies):
    """ 
    A good test to see if we can empty out the 
    bulk of the database cleanly.
    """
    test_db, ignore , ignore = django_dependencies
    collections = test_db.collections_retrieve(name_contains='posix')
    assert collections.count() == 2, "Failed to find correct number of posix collections"
    # find all the files in those collections and make sure we delete those
    for collection in collections:
        test_db.collection_delete(collection.name, force=True)
    for v in test_db.variables_all():
        assert v.in_files.count()!=0, f"Variable [{v}] should not exist if it is in NO files"
    
    






    