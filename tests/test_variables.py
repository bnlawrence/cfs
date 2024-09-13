from core.db.standalone import setup_django
from pathlib import Path
from cfdm import cellmethod
from django.db import connection

import pytest

###
### This test file concentrates on the issues around variable import and querying
###
CELL_TEST_DATA = [
     ('latitude','mean'),
     ('longitude','mean'),
     ('time', 'mean')
]

STD_DOMAIN_PROPERTIES = {'name':'N216','region':'global','nominal_resolution':'10km',
                            'size':1000,'coordinates':'longitude,latitude,pressure'}

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
    connection.close()  # Properly close the connection to the database

@pytest.fixture(scope="module")
def test_db():
    """ 
    This database (and it's contents) is used in all the following
    tests, and is progressively modified as the tests proceed.
    """
    from core.db.interface import CollectionDB
    return CollectionDB()


def test_simple_variable(test_db):
    properties = {'identity':'test var 1','atomic_origin':'imaginary','temporal_resolution':'daily',
                  'domain':STD_DOMAIN_PROPERTIES}
    var = test_db.variable_retrieve_or_make(properties)

def test_keys_with_same_value(test_db):
    properties = {'identity':'test var 2','standard_name':'test var 1','atomic_origin':'imaginary','temporal_resolution':'daily',
                  'domain':STD_DOMAIN_PROPERTIES}
    var = test_db.variable_retrieve_or_make(properties)


def test_sharing_domain(test_db):
    properties = {'identity':'test var 3','atomic_origin':'imaginary','temporal_resolution':'monthly',
                  'domain':STD_DOMAIN_PROPERTIES}
    var = test_db.variable_retrieve_or_make(properties)
    assert len(test_db.domains_all()) == 1
    assert len(test_db.variables_all()) == 3

def test_not_sharing_domain(test_db):
    domain_properties = {'name':'N216','region':'global','nominal_resolution':'10km',
                            'size':1000,'coordinates':'longitude,latitude,levels'}
    properties = {'identity':'test var 4','atomic_origin':'imaginary','temporal_resolution':'monthly',
                  'domain':domain_properties}
    var = test_db.variable_retrieve_or_make(properties)
    assert len(test_db.domains_all()) == 2
    assert len(test_db.variables_all()) == 4

def test_creating_variable_with_properties(test_db):
    properties = {'identity':'test var 5','atomic_origin':'imaginary','temporal_resolution':'daily',
                  'domain': STD_DOMAIN_PROPERTIES,
                  'experiment':'mytest','institution':'Narnia'}
    var = test_db.variable_retrieve_or_make(properties)
    assert len(test_db.variables_all()) == 5

def test_retrieving_by_property_keys(test_db):
    variables = test_db.variables_retrieve_by_key('experiment','mytest')
    assert len(variables) == 1

def test_cell_methods_create(test_db):
    """ 
    Test creating a variable with cell methods in the properties 
    """
    properties = {'identity':'test var 6','atomic_origin':'imaginary','temporal_resolution':'monthly',
                  'domain':STD_DOMAIN_PROPERTIES,
                  'cell_methods':CELL_TEST_DATA}
    var = test_db.variable_retrieve_or_make(properties)

def test_querying_cell_methods(test_db):
    """
    Find all variables with a time: mean cell method and monthly data
    """
    # first create another one to create trouble 
    props = {'temporal_resolution':'monthly', 'cell_methods':[('time','mean'),]}
    var1 = test_db.variables_retrieve_by_properties(props)
    var2 = test_db.variables_retrieve_by_properties({'identity':'test var 6'})
    assert len(var1) == 1
    assert var1[0] == var2[0]

def test_file_variable_collection(test_db):
    """
    Test adding a file and collection, then adding a variable into both,
    then retrieving the variable via it's presence in the file or collection. 
    """
    file_properties ={'name':'test_file_1','path':'/nowhere/','size':10}
    v = test_db.variables_retrieve_by_properties({'identity':'test var 6'})[0]
    c = test_db.collection_create('Holding')
    l = test_db.locations_retrieve()[0]
    f = test_db.upload_file_to_collection(l.name, c.name, file_properties)
  
    test_db.variable_add_to_file_and_collection(v, f, c.name)
    var2 = test_db.variables_retrieve_by_properties({'in_file':f})    
    assert var2[0] == v
    var2 =  test_db.variables_retrieve_by_properties({'in_file':file_properties})    
    assert var2[0] == v
    var2 = test_db.variables_retrieve_by_properties({},from_collection=c)
    assert var2[0] == v
    var2 = test_db.variables_retrieve_by_properties({'identity':'fred'}, from_collection=c)
    assert len(var2) == 0
    var2 = test_db.variables_retrieve_by_properties({'identity':'test var 6'}, from_collection=c)
    assert var2[0] == v