from core.db.cfparsing import infer_temporal_resolution, extract_cfsdomain
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



def test_infer_timing(inputfield):

    #FIXME:Horrible hack. Hourly unit tests fail if we start with cf.Data(15.,45.,75.5)
    #Why?Help?
    tconstruct = inputfield.construct('T')
    t2construct = cf.DimensionCoordinate(
                        properties={'standard_name': 'time',
                        'units': cf.Units('days since 2018-12-31')},
                        data = cf.Data([15.5,45.5,75.5]) )
    inputfield.set_construct(t2construct)
    print('bounds',inputfield.construct('T').has_bounds())

    assert infer_temporal_resolution(inputfield) == '1m'

    inputfield.construct('T')[...] = [0/24.,1./24,2./24]
   
    assert infer_temporal_resolution(inputfield) == '1h'

    inputfield.construct('T')[...] = [0,3./24,6./24]
    assert infer_temporal_resolution(inputfield) == '3h'

    inputfield.construct('T')[...] = [45,135,225]
    assert infer_temporal_resolution(inputfield) == '3m'

    inputfield.construct('T')[...] = [0,360,720]
    assert infer_temporal_resolution(inputfield) == '1y'

def test_domain(inputfield):
    
    d = extract_cfsdomain(inputfield)
    assert d['name'] == 'test'

def test_atomic_name(inputfield):

    aname = parse2atomic_name(inputfield)
    assert aname == 'testing/NCAS'

def test_field_parsing(inputfield):

    adict = parse_fields_todict([inputfield], temporal_resolution=None, lookup_class=None)[0]
    assert adict['identity'] == 'specific_humidity'
    assert 'units' in adict['_proxied']
    assert adict['temporal_resolution'] == '1m'
    assert adict['domain']['name'] == 'test'
    assert 'atomic_origin' in adict

def test_upload_parsed_dict(inputfield, test_db):
    """ Can we sensibly upload a variable from this parsed dictionary?"""
    adict = parse_fields_todict([inputfield], temporal_resolution=None, lookup_class=None)[0]
    v0=test_db.variable_retrieve_or_make(adict)
    with pytest.raises(ValueError):
       # this should fail because we can't do a retrieve or make without enough info to do a make!
       v1 = test_db.variable_retrieve_or_make({'identity':'specific_humidity'})
    v1 = test_db.variables_retrieve_by_properties({'identity':'specific_humidity'})[0]
    # I can't seem to get proper test isolation, so for now, let's just delete the things
    # that intefere with other tests.
    print('parsing deletion attempt')
    v1.delete()

  
