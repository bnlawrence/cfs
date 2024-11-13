from pathlib import Path
from cfs.db.cfa_tools import CFAhandler
from cfs.db.cfparsing import parse_fields_todict

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


#    cf.write
#    
#    
#    f1 = inputfield
#    f2 = f1.copy()
#    f3 = f1.copy()
#
#    # there will be a more elegant way of doing this, but this is fine for now
#    new_dates1 = np.array([105,135,165])
#    new_bounds1 = np.array([[91,120],[121,150],[151,180]])
#    new_dates2 = new_dates1 + 90
#    new_bounds2 = new_bounds1 + 90
#
#    for nd, nb, f in zip([new_dates1,new_dates2], [new_bounds1, new_bounds2],[f2,f3]):
#        dimT = cf.DimensionCoordinate(
#                    properties={'standard_name': 'time',
#                                'units':   cf.Units('days since 2018-12-30',calendar='360_day')},
#                    data=cf.Data(nd),
#                    bounds=cf.Bounds(data=cf.Data(nb))
#                    )
#        f.set_construct(dimT)
#
#    fields = [f1,f2,f3]

    filenames = [posix_path/f'test_file{x}.nc' for x in range(3)]
    print(inputfield)
    for f, index in zip(filenames, range(0, 36, 12)):
        print(index, f )
        cf.write(inputfield[index:index+12], f)
                                     
    print ('YYYYYY', list(posix_path.glob('*.nc')))
    f = cf.read(posix_path.glob('*.nc'), cfa_write='field')[0]
    print(f)
#    f.data.nc_update_aggregation_substitutions({'base': f"{posix_path}/"})
    
    cfa_file = str(posix_path/'test_file.cfa')
    #FIXME: I don't think the substitutions are being parsed properly.
    cf.write(f, cfa_file, cfa={"constructs": "field", "uri": "relative"}) 
#                               'substitutions':{'base':'./'}})
    print('CFA setup with three files and one field')
    g = cf.read(cfa_file)
    print ('g.filenames',g[0].get_filenames())
    
    return posix_path

def test_cfa_handler(cfa_only):

    fields = cf.read(cfa_only.glob('*.cfa'))
    print ('FFFFF', list(    cfa_only.glob('*.cfa')))
    print (fields)
    
    c = CFAhandler(expected_fields=len(fields))

    for f in fields:
        print(f)
        print (f.array)
        print (f.get_filenames())
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




    

