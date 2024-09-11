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

@pytest.fixture
def inputfield():
    """ 
    Create a field to use for testing. This is nearly straight from the CF documentation.
    """

    # Initialise the field construct with properties
    Q = cf.Field(properties={'project': 'testing', 'institution':'NCAS',
                            'standard_name': 'specific_humidity',
                            'units': '1'})

    # Create the domain axis constructs
    domain_axisT = cf.DomainAxis(3)
    domain_axisY = cf.DomainAxis(5)
    domain_axisX = cf.DomainAxis(8)

    # Insert the domain axis constructs into the field. The
    # set_construct method returns the domain axis construct key that
    # will be used later to specify which domain axis corresponds to
    # which dimension coordinate construct.
    axisT = Q.set_construct(domain_axisT)
    axisY = Q.set_construct(domain_axisY)
    axisX = Q.set_construct(domain_axisX)

    # Create and insert the field construct data
    data = cf.Data(np.arange(40.).reshape(5, 8))
    Q.set_data(data)

    # Create the cell method constructs
    cell_method1 = cf.CellMethod(axes='area', method='mean')

    cell_method2 = cf.CellMethod()
    cell_method2.set_axes(axisT)
    cell_method2.set_method('maximum')

    # Insert the cell method constructs into the field in the same
    # order that their methods were applied to the data
    Q.set_construct(cell_method1)
    Q.set_construct(cell_method2)

    # Create a "time" dimension coordinate construct with no bounds
    tdata = [15.5,44.5,75.]
    dimT = cf.DimensionCoordinate(
                                properties={'standard_name': 'time',
                                            'units': 'days since 2018-12-01'},
                                data=cf.Data(tdata)
                                )
    # Create a "longitude" dimension coordinate construct, without
    # coordinate bounds
    dimX = cf.DimensionCoordinate(data=cf.Data(np.arange(8.)))
    dimX.set_properties({'standard_name': 'longitude',
                        'units': 'degrees_east'})

    # Create a "longitude" dimension coordinate construct
    dimY = cf.DimensionCoordinate(properties={'standard_name': 'latitude',
                                            'units'        : 'degrees_north'})
    array = np.arange(5.)
    dimY.set_data(cf.Data(array))

    # Create and insert the latitude coordinate bounds
    bounds_array = np.empty((5, 2))
    bounds_array[:, 0] = array - 0.5
    bounds_array[:, 1] = array + 0.5
    bounds = cf.Bounds(data=cf.Data(bounds_array))
    dimY.set_bounds(bounds)

    # Insert the dimension coordinate constructs into the field,
    # specifying to which domain axis each one corresponds
    Q.set_construct(dimT)
    Q.set_construct(dimY)
    Q.set_construct(dimX)

    return Q

def test_infer_timing(inputfield):

    assert infer_temporal_resolution(inputfield) == '1m'

    inputfield.construct('T')[...] = [0,1./24,2./24]
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
    v1.domain.delete()
    v1.delete()

  
