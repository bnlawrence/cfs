from pathlib import Path
import json
import pytest
import numpy as np

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
        m['bounds'] = np.array(m['bounds'])
        maniset[m['manikey']] = m
    filedata={'properties':cfafile,
              'manifests':maniset,
              'variables':data['variables']}
    test_db.collection.create(name='canari_fail1')
    test_db.upload_file_to_collection('canari_eg1','canari_fail1',filedata)


    