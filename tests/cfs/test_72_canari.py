from pathlib import Path
import json, io
import pytest
import numpy as np
import cf

from cfs.db.standalone import setup_django

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

@pytest.fixture
def test_db():
    """ 
    This database (and it's contents) is used in all the following
    tests, and is progressively modified as the tests proceed.
    """
    from cfs.db.interface import CollectionDB
    return CollectionDB()


def test_canari_eg1(test_db):
    """ 
    These are the first few dictionaries from a real CANARI use
    case. We will hack in a file reference and attempt a file upload.
    In practice these caused an integrity error that this unit test
    is designed to replicate so that it wont happen again.
    """
    jsonfile = Path(__file__).parent.resolve()/'canari_eg1.json'
    with open(jsonfile,'r') as f:
        data = json.load(f)
    cfafile = {'name':'myfile1','path':'/tmp/myfile1','size':10,'type':'S','location':'init'}
    manifests = data['manifests']
    maniset = {}
    for m in manifests:
        binary = io.BytesIO()
        np.save(binary, np.array(m['bounds']))
        m['bounds'] = binary.getvalue()
        maniset[m['manikey']] = m
    filedata={'properties':cfafile,
              'manifests':maniset,
              'variables':data['variables']}
    test_db.collection.create(name='canari_fail1')
    test_db.location.create(name='canari_eg1')
    test_db.upload_file_to_collection('canari_eg1','canari_fail1',filedata)


def test_quarks(django_dependencies):
    test_db, ignore , ignore = django_dependencies
    vars = test_db.variable.all()
    v = vars[0]
    td = v.time_domain
    
    #check get sensibele results from trying to get the same thing
    starting = cf.Data(td.starting, units=cf.Units(td.units, calendar=td.calendar))
    ending = cf.Data(td.ending, units=cf.Units(td.units, calendar=td.calendar))
    manifest = v.in_manifest
    print(f'variable {v} has manifest {manifest}')
    assert manifest is not None
    print(starting, ending)
    quark = test_db.manifest.make_quark(manifest, starting, ending)
    assert quark.id == manifest.id
    
    


    