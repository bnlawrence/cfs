import cf
import pytest
import numpy as np

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
    data = cf.Data(np.arange(120.).reshape(3, 5, 8))
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