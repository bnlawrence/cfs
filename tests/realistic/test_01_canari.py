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
def test_dbcan():
    """ 
    This database (and it's contents) is used in all the following
    tests, and is progressively modified as the tests proceed.
    """
    from cfs.db.interface import CollectionDB
    from cfs.plugins.posix import Posix, get_parent_paths
    db = CollectionDB()
    return db,Posix(db,'cananri-eg')


def NOtest_canari_eg1(test_db):
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
    test_db.upload_file_to_collection('canari_eg1','scanari_fail1',filedata)

def test_canari_eg(test_dbcan):
    test_db, p = test_dbcan
    current_file_path = Path(__file__).resolve().parent
    data_dir = current_file_path/'data'
    collection='canari-cutdown'
    description='test data'
    p.add_collection(data_dir,
                      collection,
                      description,
                      regex='*.cfa',
                      intent='A')
    assert test_db.variable.count() == 2


def test_retrieving1(django_dependencies):
    
    test_db, _, _ = django_dependencies
    vars = test_db.variable.retrieve_by_properties({'standard_name':'eastward_wind'})
    assert len(vars) > 1

def test_retrieve_by_properties(django_dependencies):
    test_db, _, _ = django_dependencies
    vars0 = test_db.variable.all()
    vars1 = test_db.variable.retrieve_by_properties({'standard_name':'eastward_wind'})
    vars2 = test_db.variable.retrieve_by_properties({'frequency':'6hr_pt'})
    vars3 = test_db.variable.retrieve_by_properties({'standard_name':'eastward_wind',
                                                        'frequency':'6hr_pt'})
    len0 = len(vars0)
    len1 = len(vars1)
    len2 = len(vars2)
    len3 = len(vars3)

    print([(v.get_kp('standard_name'),v.get_kp('frequency')) for v in vars0])
    print([(v.get_kp('standard_name'),v.get_kp('frequency')) for v in vars1])
    print([(v.get_kp('standard_name'),v.get_kp('frequency')) for v in vars2])
    print([(v.get_kp('standard_name'),v.get_kp('frequency')) for v in vars3])


    print(len0, len1, len2, len3)

    print (1/0)

def test_canari_time_domains(django_dependencies):

    test_db, _, _ = django_dependencies
    td = test_db.tdomain.all()
    for t in td:
        print(t)


def test_quarks(django_dependencies):
    test_db, _, _ = django_dependencies
    vars = test_db.variable.all()
    v = vars[0]
    td = v.time_domain
    print(td)
    
    #check get sensibele results from trying to get the same thing
    starting = cf.Data(td.starting, units=cf.Units(td.units, calendar=td.calendar))
    ending = cf.Data(td.ending, units=cf.Units(td.units, calendar=td.calendar))
    manifest = v.in_manifest
    assert manifest is not None
    quark, created = test_db.manifest.subset(manifest, starting, ending)
    assert quark.id == manifest.id
    assert quark.is_quark is False

    #ok, now see if we can get a subset
    ending = cf.Data(10., units=cf.Units(
        'days since 1970-11-30',calendar='360_day'))
    quark, created = test_db.manifest.subset(manifest, starting, ending)
    assert quark.id != manifest.id
    assert quark.is_quark is True
    print(quark)
    expected = 'cn134a_999_6hr_u_pt_cordex__197012-197012.nc'
    fragments = list(quark.fragments.files.all())
    assert expected == fragments[-1].name


def test_var_subset(django_dependencies):

    test_db, _, _ = django_dependencies
    vars = test_db.variable.all()
    v = vars[0]
    vtunits = v.time_domain.units
    q, c, m, t = test_db.variable.subset(v, (1,2,1950),(1,1,1960))
    assert c==m==t==True
    assert q.time_domain.units == vtunits


def test_collection_quarks(django_dependencies):
    """ Basic quark creation and deletion"""
    test_db, _, _ = django_dependencies
    vars = test_db.variable.all()
    test_db.collection.make_quarks('quarktest1',(1,2,1950),(1,1,1954), vars)
    QuarkTag = test_db.tag.retrieve(name='Quark')
    qt1 = test_db.collection.retrieve(name='quarktest1')
    assert QuarkTag in qt1.tags.all()
    test_db.collection.delete('quarktest1')


def test_collection_quarks2(django_dependencies):
    """ 
    This is really to demonstrate we can do test3 if we have no
    quarktest1 first
    """
    test_db, _, _ = django_dependencies
    vars = test_db.variable.retrieve_by_properties({'standard_name':'eastward_wind',
                                                    'frequency':'6hr_pt'})
    test_db.collection.make_quarks('quarktest2',(1,2,1955),(1,1,1960), vars)
    test_db.collection.delete('quarktest2')

def test_collection_quarks3(django_dependencies):
    """
    Now see if having non-overlapping quarks in the database breaks things
    """
    test_db, _, _ = django_dependencies
    vars = test_db.variable.all()
    test_db.collection.make_quarks('quarktest3',(1,2,1950),(1,1,1954), vars)
    vars = test_db.variable.retrieve_by_properties({'standard_name':'eastward_wind',
                                                    'frequency':'6hr_pt'})
    print('Quarktest4 will use ',vars)
    test_db.collection.make_quarks('quarktest4',(1,2,1955),(1,1,1960), vars)





    

       

    
    
    


    