from pathlib import Path
from core.db.cfa_tools import CFAhandler
from core.db.cfparsing import parse_fields_todict

import cf
import numpy as np
import pytest

@pytest.fixture
def cfa_only(tmp_path, inputfield):
    """
    Make three copies of our basic netcdf file, and then aggregate it
    """

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
    cfa_file = posix_path/'test_file.cfa'
    #FIXME: I don't think the substitutions are being parsed properly.
    cf.write(f, cfa_file, cfa={'absolute_paths':False,
                               'substitutions':{'base':'./'}})
    print('CFA setup with three files and one field')

    return posix_path

def test_cfa_handler(cfa_only):

    fields = cf.read(cfa_only.glob('*.cfa'))

    c = CFAhandler(expected_fields=len(fields))

    for f in fields:

        c.parse_field_to_manifest(f)

    assert len(c.known_manifests) == 1

def test_variable_manifest_linkage(cfa_only):
    """ 
    The manikey should link variables to manifests found in the cfa file
    """
    fields = cf.read(cfa_only.glob('*.cfa'))

    descriptions, manifests = parse_fields_todict(fields[0:10], cfa=True)

    vkey = descriptions[0].pop('manikey')
    mkey = list(manifests.keys())[0]
    assert vkey == mkey




    

