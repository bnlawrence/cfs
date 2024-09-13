import cf
import pytest
from pathlib import Path
import os

from core.db.standalone import setup_django

VARIABLE_LIST = ['specific_humidity',]

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
def cfa_resources(tmp_path, inputfield):
    posix_path = tmp_path / 'posix_root'  
    posix_path.mkdir(exist_ok=True)  
    
    f1 = inputfield
    f2 = f1.copy()
    f2t = f2.construct('T')
    new_dates = [105,135,165]
    new_bounds = [[91,120],[121,150],[151,180]]
    dimT = cf.DimensionCoordinate(
                properties={'standard_name': 'time',
                            'units':   cf.Units('days since 2018-12-30',calendar='360_day')},
                data=cf.Data(new_dates),
                bounds=cf.Bounds(data=cf.Data(new_bounds))
                )
    f2.set_construct(dimT)

    fields = [f1,f2]

    filenames = [posix_path/f'test_file{x}.nc' for x in range(2)]
    for v,f in zip(fields,filenames):
        cf.write([v,],f)

    f = cf.read(posix_path.glob('*.nc'))
    cfa_file = posix_path/'test_file.cfa'
    cf.write(f, cfa_file, cfa=True)

    return posix_path

def test_cfa_view(django_dependencies, cfa_resources):
    """ 
    Test uploading a cfa file as normal nc file
    """
    #for f in cfa_resources.glob('*.cfa'):
    #    os.system(f'ncdump {f}')
    posix_path = cfa_resources
    test_db, p, ignore = django_dependencies
    p.add_collection(
        str(posix_path),
        'posix_cfa_example',
        'one aggregated variable',
        match='*.cfa'
        )
    c1 = test_db.collection_retrieve('posix_cfa_example')
    assert set([x.standard_name.value for x in c1.variables.all()]) == set(VARIABLE_LIST)


def test_cfa_cleanup(django_dependencies):
    """ 
    A good test to see if we can empty out the 
    bulk of the database cleanly.
    """
    test_db, ignore , ignore = django_dependencies
    collections = test_db.collections_retrieve(name_contains='posix')
    assert collections.count() == 1, "Failed to find correct number of posix collections"
    # find all the files in those collections and make sure we delete those
    for collection in collections:
        test_db.collection_delete(collection.name, force=True)
    for v in test_db.variables_all():
        assert v.in_files.count()!=0, f"Variable [{v}] should not exist if it is in NO files"
    test_db.location_delete('vftesting')
    # Eventually we need to check we empty the fragment files too.

