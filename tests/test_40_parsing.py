from core.db.cfparsing import LookupT, extract_cfsdomain
from core.db.cfparsing import parse_fields_todict, parse2atomic_name
from core.db.standalone import setup_django
from django.db import connection
from pathlib import Path

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
    from core.db.interface import CollectionDB
    return CollectionDB()

def test_infer_timing():

    #FIXME:Horrible hack. Hourly unit tests fail if we start with cf.Data(15.,45.,75.5)
    #Why?Help?
    units = cf.Units('days since 2018-12-31')
    t2construct = cf.DimensionCoordinate(
                        properties={'standard_name': 'time',
                        'units': units},
                        data = cf.Data([15.5,45.5,75.5],units=units) )
 
    lookup = LookupT()

    
    r = lookup.infer_interval_from_coord(t2construct) == (1,'m')
    print(r)
    assert r == 1,'m'

    t2construct[...] = [0/24.,1./24,2./24]
    r = lookup.infer_interval_from_coord(t2construct) == (1,'h')
    assert r == 1,'h'

    t2construct[...] = [0,3./24,6./24]
    assert lookup.infer_interval_from_coord(t2construct) == (3,'h')

    t2construct[...] = [45,135,225]
    assert lookup.infer_interval_from_coord(t2construct) == (3,'m')

    t2construct[...] = [0,360,720]
    assert lookup.infer_interval_from_coord(t2construct) == (1,'y')

def test_domain(inputfield):
    
    print(inputfield)
    d = extract_cfsdomain(inputfield)
    assert d['name'] == 'test'

def test_atomic_name(inputfield):

    aname = parse2atomic_name(inputfield)
    assert aname == 'testing/NCAS'

def test_field_parsing(inputfield):

    adict = parse_fields_todict([inputfield], temporal_resolution=None, lookup_xy=None)[0]
    assert adict['identity'] == 'specific_humidity'
    assert 'units' in adict['_proxied']
    assert adict['time_domain']['interval'] == 1
    assert adict['spatial_domain']['name'] == 'test'
    assert 'atomic_origin' in adict

def test_upload_parsed_dict(inputfield, test_db):
    """ Can we sensibly upload a variable from this parsed dictionary?"""

    print('Known domains',test_db.xydomain.all())
    file_properties ={'name':'test_file_1','path':'/nowhere/','size':10,'location':'parloc'}

    l = test_db.location.create('parloc')
    c = test_db.collection.create(name='parcol')
 
    adict = parse_fields_todict([inputfield], temporal_resolution=None, lookup_xy=None)[0]
    adict['identity'] = 'special test var'
    
    filedata={'properties':file_properties, 'variables':[adict,]}
    test_db.upload_file_to_collection(l.name, c.name, filedata)
  

def test_cleanup(test_db):


    with pytest.raises(ValueError):
       # this should fail because we can't do a retrieve or make without enough info to do a make!
       v1 = test_db.variable.get_or_create({'identity':'special test var'})
    v1 = test_db.variable.retrieve_by_properties({'identity':'special test var'})[0]
    # Tests are not necessarily isolated, so for now, let's just delete the things
    # that intefere with other tests from other files.
    
    d1 = v1.spatial_domain.name
    v1.delete()
    d2 = test_db.xydomain.retrieve_by_name(d1)
    assert d2 is None, 'Variable delete failed to delete the spatial domain used for parsing'
    d = test_db.xydomain.count()
    assert d==0,'parsing cleanup not complete'
  

