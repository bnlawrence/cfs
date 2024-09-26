from cfs.db.standalone import setup_django
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

STD_DOMAIN_PROPERTIES = {'name':'TestDomain','region':'global','nominal_resolution':'10km',
                            'size':1000,'coordinates':'longitude,latitude,pressure'}
STD_TEMPORAL = {'interval':30,'starting':15.,'ending':345, 
                'units':'days since 2018-12-30','calendar':'360_day'}
DAILY_TEMPORAL =  {'interval':1,'starting':1.,'ending':30., 
                'units':'days since 2018-12-30','calendar':'360_day'}
FILE_PROPERTIES = {'name':'test_file_1','path':'/nowhere/','size':10,'location':'varloc'}


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
  

@pytest.fixture(scope="module")
def test_data():
    """ 
    This database (and it's contents) is used in all the following
    tests, and is progressively modified as the tests proceed.
    """
    from cfs.db.interface import CollectionDB
    cdb = CollectionDB()
    col = cdb.collection.create(name='Holding')
    file = cdb.file.create(FILE_PROPERTIES)
    return cdb, file, col

def test_simple_variable(test_data):
    test_db, file, col = test_data
    properties = {'identity':'test var 1','atomic_origin':'imaginary', 'in_file':file,
                  'spatial_domain':STD_DOMAIN_PROPERTIES, 'time_domain':STD_TEMPORAL,}
    var = test_db.variable.get_or_create(properties)
    assert var.get_kp('identity') == 'test var 1'
    assert var.get_kp('atomic_origin') == 'imaginary'
    loc = test_db.location.retrieve('varloc')
    assert loc.volume == FILE_PROPERTIES['size']
    print(var.dump())
    
def test_unique_variable_insertion(test_data):
    """
    Test that unique variables with different key properties can be inserted.
    Sharing domains!
    """
    test_db, file, _ = test_data

    properties_a = {
        'identity': 'var A',
        'atomic_origin': 'origin A',
        'in_file': file,
        'spatial_domain': STD_DOMAIN_PROPERTIES,
        'time_domain': STD_TEMPORAL
    }
    
    properties_b = {
        'identity': 'var B',
        'atomic_origin': 'origin B',
        'in_file': file,
        'spatial_domain': STD_DOMAIN_PROPERTIES,
        'time_domain': STD_TEMPORAL
    }

    var_a = test_db.variable.get_or_create(properties_a)
    var_b = test_db.variable.get_or_create(properties_b)
    
    assert var_a.get_kp('identity') == 'var A'
    assert var_b.get_kp('identity') == 'var B'
    assert test_db.xydomain.count() == 1

def test_duplicate_variable_rejected(test_data):
    """
    Test that inserting a duplicate variable raises an error.
    """
    test_db, file, _ = test_data

    properties = {
        'identity': 'duplicate var',
        'atomic_origin': 'same origin',
        'in_file': file,
        'spatial_domain': STD_DOMAIN_PROPERTIES,
        'time_domain': STD_TEMPORAL
    }

    # Insert the first variable
    var_1 = test_db.variable.get_or_create(properties)

    # Attempt to insert the same variable again
    with pytest.raises(PermissionError):
        var_2 = test_db.variable.get_or_create(properties)


def test_get_or_create_unique_instance(test_data):
    """
    Test the get_or_create_unique_instance method to ensure uniqueness checks are working.
    """
    test_db, file, _ = test_data

    properties = {
        'identity': 'unique var',
        'atomic_origin': 'unique origin',
        'in_file': file,
        'spatial_domain': STD_DOMAIN_PROPERTIES,
        'time_domain': STD_TEMPORAL
    }

    var = test_db.variable.get_or_create(properties)
    assert var.get_kp('identity') == 'unique var'

    # Try creating it again (should return the same instance, not create a new one)
    var_duplicate = test_db.variable.get_or_create(properties, unique=False)
    assert var == var_duplicate

    with pytest.raises(PermissionError):
        var_duplicate = test_db.variable.get_or_create(properties, unique=True)
    


def test_keys_with_same_value(test_data):
    test_db, f, c  = test_data
    properties = {'identity':'test var 2','standard_name':'test var 1','atomic_origin':'imaginary',
                   'spatial_domain':STD_DOMAIN_PROPERTIES, 'in_file':f,'time_domain':STD_TEMPORAL}
    var = test_db.variable.get_or_create(properties)


def test_not_sharing_domain(test_data):
    test_db, f, c  = test_data
    domain_properties = {'name':'AnotherDomain','region':'global','nominal_resolution':'20km',
                            'size':1000,'coordinates':'longitude,latitude,levels'}
    properties = {'identity':'test var 4','atomic_origin':'imaginary','in_file':f,
                  'spatial_domain':domain_properties,'time_domain':STD_TEMPORAL}
    var = test_db.variable.get_or_create(properties)
    db = test_db.xydomain.all()
    assert test_db.xydomain.count() == 2

def test_creating_variable_with_properties(test_data):
    test_db, f, c  = test_data
    properties = {'identity':'test var 5','atomic_origin':'imaginary',
                  'spatial_domain': STD_DOMAIN_PROPERTIES, 'time_domain':DAILY_TEMPORAL,
                  'experiment':'mytest','institution':'Narnia','in_file':f}
    var = test_db.variable.get_or_create(properties)
    assert var['institution'] == 'Narnia'

def test_retrieving_by_property_keys(test_data):
    test_db, f, c  = test_data
    variables = test_db.variable.retrieve_by_key('experiment','mytest')
    assert len(variables) == 1

def test_cell_methods_create(test_data):
    """ 
    Test creating a variable with cell methods in the properties 
    """
    test_db, f, c  = test_data
    properties = {'identity':'test var 6','atomic_origin':'imaginary',
                  'spatial_domain':STD_DOMAIN_PROPERTIES,'time_domain':STD_TEMPORAL,
                  'cell_methods':CELL_TEST_DATA, 'in_file':f}
    var = test_db.variable.get_or_create(properties)

def test_querying_cell_methods(test_data):
    """
    Find all variables with a couple of combinations of cell methods
    """
    test_db, f, c  = test_data
    # first create another one to create trouble 
    properties = {'identity':'test var 7','atomic_origin':'imaginary',
                  'spatial_domain':STD_DOMAIN_PROPERTIES,'time_domain':STD_TEMPORAL,
                  'cell_methods':[('time','mean'),], 'in_file':f}
    var = test_db.variable.get_or_create(properties)
    vars = test_db.variable.all()
    var1 = test_db.variable.retrieve_by_properties({'cell_methods':CELL_TEST_DATA})
    assert len(var1) == 1
    var2 = test_db.variable.retrieve_by_properties({'identity':'test var 6'})
    assert var1[0] == var2[0]
    vars = test_db.variable.retrieve_by_properties({'cell_methods':[('time','mean'),]})
    assert len(vars) == 2

def test_more_queries(test_data):
    """
    Test adding a file and collection, then adding a variable into both,
    then retrieving the variable via it's presence in the file or collection. 
    """
    test_db, f, c  = test_data
    vars = test_db.variable.retrieve_by_properties({'identity':'test var 1'})
    assert len(vars) == 1,'Failed to recover the first variable identified by identity property'
    v = vars[0]   
    var2 = test_db.variable.retrieve_by_properties({'in_file':f})    
    assert var2[0] == v,'Failed to recover the first file created by file instance'
    file_properties ={'name':'test_file_1','path':'/nowhere/','size':10}
    var2 =  test_db.variable.retrieve_by_properties({'in_file':file_properties})    
    assert var2[0] == v,f'Failed to recover the first file created by properties'
    

def test_variables_and_collections(test_data):
    test_db, f, c  = test_data
    vars = test_db.variable.all()
    # add the first three to collection
    for v in vars[0:3]:
         test_db.variable.add_to_collection(c.name, v)
    var2 = test_db.variable.retrieve_by_properties({},from_collection=c)
    assert len(var2) == 3, f"Expected the 3 variables added here to be returned, got {len(var2)}"
    var2 = test_db.variable.retrieve_by_properties({'identity':'fred'}, from_collection=c)
    assert len(var2) == 0,f'Expected to find no variables with identity fred, but got {len(var2)}'
    var2 = test_db.variable.retrieve_by_properties({'identity':'test var 1'}, from_collection=c)
    assert len(var2) == 1, f'Expected to recover just the one file from collection {c}'
    assert var2[0] == vars[0],f'Expected to recover the first variable from collection {c}'

def test_deletion(test_data):

    test_db, f, c  = test_data
    spatial = test_db.xydomain.retrieve_by_name('AnotherDomain')
    vars = test_db.variable.retrieve_by_queries([('spatial_domain',spatial),])

    loc = test_db.location.retrieve('varloc')
    files = test_db.file.in_location('varloc')
    for f in files: print(f)
    assert loc.volume == sum([f.size for f in files])

    to_die = []
    count = len(vars)
    for v in vars:
        to_die.append(v.get_kp('identity'))
        v.delete()

    loc = test_db.location.retrieve('varloc')
    files = test_db.file.in_location('varloc')
    assert loc.volume == sum([f.size for f in files])

    spatial = test_db.xydomain.retrieve_by_name('AnotherDomain')
    assert spatial is None, 'Failed to delete spatial domain as variable was deleted'

def test_cleanup(test_data):
    """ Should be deleting all this stuff that depends on these variables"""

    test_db, f, c = test_data

    for v in test_db.variable.all():
        loc = test_db.location.retrieve('varloc')
        #print(f'Deleting {v} which is in file {v.in_file} with {v.in_file.variable_set.count()}, {(loc.volume)}')
        v.delete()
    
    assert test_db.variable.count() == 0
    assert test_db.xydomain.count() == 0
    assert test_db.tdomain.count() == 0



    