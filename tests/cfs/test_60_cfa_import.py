import cf
import pytest
from pathlib import Path
import numpy as np
import os

from cfs.db.standalone import setup_django


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
    """
    Make three copies of our basic netcdf file, and then aggregate it
    """

    def fix_filenames(ncfile):
        import xarray as xr

        # open your dataset
        ds = xr.open_dataset(ncfile)

        # change an existing variable
        base = '${base}'
        shape = ds['cfa_file'].shape
        new_filenames = np.vectorize(lambda f:f'{base}:{f}')(ds['cfa_file'].values)
        reshaped_filenames = np.reshape(new_filenames, shape)
        
        ds['cfa_file'] = xr.DataArray(reshaped_filenames,
                                      dims=ds['cfa_file'].dims)

        # Ensure the _FillValue is correctly set for the specific_humidity variable
        if '_FillValue' not in ds['specific_humidity'].encoding:
            ds['specific_humidity'].encoding['_FillValue'] = None  # or None if it should remain blank
        
        # write a new file
        ofile = str(ncfile).replace('.nc','.cfa')
        ds.to_netcdf(ofile, mode='w')
        os.remove(ncfile)
        os.system(f'ncdump {ofile}')



    posix_path = tmp_path / 'posix_root'  
    posix_path.mkdir(exist_ok=True)  
    
    f1 = inputfield
    f2 = f1.copy()
    f3 = f1.copy()

    # there will be a more elegant way of doing this, but this is fine for now
    new_dates1 = np.array([105,135,165])
    new_bounds1 = np.array([[91,120],[121,150],[151,180]])
    new_dates2 = new_dates1 + 90
    new_bounds2 = new_bounds1 + 90

    for nd, nb, f in zip([new_dates1,new_dates2], [new_bounds1, new_bounds2],[f2,f3]):
        dimT = cf.DimensionCoordinate(
                    properties={'standard_name': 'time',
                                'units':   cf.Units('days since 2018-12-30',calendar='360_day')},
                    data=cf.Data(nd),
                    bounds=cf.Bounds(data=cf.Data(nb))
                    )
        f.set_construct(dimT)

    fields = [f1,f2,f3]

    filenames = [posix_path/f'test_file{x}.nc' for x in range(3)]
    for v,f in zip(fields,filenames):
        cf.write([v,],f)

    f = cf.read(posix_path.glob('*.nc'))
    cfa_file = posix_path/'test_file.nc'
    #FIXME: I don't think the substitutions are being parsed properly.
    
    cf.write(f, cfa_file, cfa={'absolute_paths':False,
                               'substitutions':{'base':'./'}})
    fix_filenames(cfa_file)
   
    print('CFA setup with three files and one field')

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
        regex='*.cfa',
        intent='C'
        )
    c1 = test_db.collection.retrieve_by_name('posix_cfa_example')
    assert set([x.get_kp('standard_name') for x in c1.variables.all()]) == set(VARIABLE_LIST)


def test_fragments(django_dependencies):
    """ 
    When we added the cfa file we should have added
    some fragments, let's look at them. Assumptions
    are that the fragments should not appear in 
    collections, but the cfa file should.
    """
    test_db, ignore, ignore = django_dependencies

    fragments = test_db.file.retrieve_all(type='F')
    files = test_db.file.retrieve_all(type='C')

    assert len(fragments) == 3
    assert len(files) == 1


def test_manifest(django_dependencies):

    test_db, ignore, ignore = django_dependencies
    manifests = test_db.manifest.all()
    assert test_db.manifest.count() == 1

    assert manifests[0].fragments.count() == 3


def test_variable(django_dependencies):

    test_db, ignore, ignore = django_dependencies
    v = test_db.variable.all()
    assert len(v) == 1
    v = v[0]
    files = test_db.file.retrieve_all(type='C')
    f = files[0]
    assert v.in_file == f
    m = test_db.manifest.all()[0]
    assert v.in_manifest == m
    


def test_deletion(django_dependencies):

    test_db, ignore , ignore = django_dependencies
    collections = test_db.collection.retrieve(name_contains='posix')
    assert collections.count() == 1, "Failed to find correct number of posix collections"

    #first test whether or not a collection delete without forcing does the right thing
    collection = collections[0]

    # we assume there are only variable in the collection of interest, but start by checking that
    n_all_variables = test_db.variable.count()
    n_this_collection = test_db.variable.retrieve_in_collection(collection.name).count()
    assert n_all_variables == n_this_collection

    # expect this to raise an error as the collection contains the last copy of variables
    with pytest.raises(PermissionError):
        collection.delete()

    #ok now test that deleting it with force, does remove the variable, files, and fragments.
    collection.delete(force=True)

    with pytest.raises(ValueError):
        test_db.collection.retrieve_by_name('posix_cfa_example')

    n_all_variables = test_db.variable.count()
    assert n_all_variables == 0

    files = test_db.file.findall_by_type('C')
    assert files.count() == 0
    
    frags = test_db.file.findall_by_type('F')
    assert frags.count() == 0


   
    


   


