from cfs.db.standalone import setup_django
from pathlib import Path

import pytest

###
### This test file concentrates on the basics of files, collections, tags, relationships and 
### locations.
###

def _dummy(db, location='testing', collection_stem="fdummy", files_per_collection=10):
    """ 
    Set up a dummy dataset in db with accessible structure for testing
    """
    loc = test_db.location.get_or_create(name=location)
    for i in range(5):
        files = [{'path': '/somewhere/in/unix_land', 'name': f'file{j}{i}', 'size': 10, 'location':loc} for j in range(files_per_collection)]
        for f in files:
            db.file.create(**f)

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
   
def test_collection_create(test_db):
    kw = {'name':'mrun1','resolution': 'N512', 'workspace': 'testws'}
    info = test_db.collection.create(**kw)
    assert info.name == 'mrun1'
    assert info.description == '(none)'

def test_delete_empty_collection(test_db):
    info = test_db.collection.create(name='death becomes her')
    test_db.collection.delete(info)


def test_unique_collection(test_db):
    kw = {'resolution': 'N512', 'workspace': 'testws'}
    test_db.collection.create(name='mrun2', **kw)
    with pytest.raises(ValueError) as context:
        test_db.collection.create(name='mrun2',**kw)
    assert 'unique constraint' in str(context)

def test_fileupload(test_db):
    """ Test uploading files """
    test_db.collection.create(name='mrun3')
    loc=test_db.location.create('testing')
    files = [{'path': '/somewhere/in/unix_land', 'name': f'filet{i}', 'size': 10,'location':loc} for i in range(10)]
    for f in files:
        test_db.file.create(f)

def test_add_and_retrieve_tag(test_db):
    """
    Need to add tags, and select by tags
    """
    tagname = 'test_tag'
    test_db.tag.create(tagname)
    test_db.tag.add_to_collection('mrun1', tagname)
    test_db.tag.add_to_collection('mrun3', tagname)
    tagged = test_db.collection.retrieve_all(tagname=tagname)
    assert ['mrun1', 'mrun3'] == [x.name for x in tagged]

def test_get_collections(test_db):
    """
    Test ability to get a subset of collections via name and/or description
    """
    for i in range(5):
        test_db.collection.create(name=f'dummy{i}')
        test_db.collection.create(name=f'eg{i}')
    test_db.collection.create(name='dummy11',description='actual description')
    assert len(test_db.collection.retrieve_all(name_contains='g')) == 5
    assert len(test_db.collection.retrieve_all(description_contains='actual')) ==  1
    with pytest.raises(ValueError) as context:
        test_db.collection.retrieve_all(description_contains='actual', name_contains='x')
    assert 'Cannot search' in str(context)


def test_collection_properties(test_db):
    """
    Test ability to create properties on collections and query against them.
    """
    choice = ['dummy2', 'dummy3']
    for c in choice:
        cc = test_db.collection.retrieve(name=c)
        cc['color'] = 'green'
        cc.save()
    r = test_db.collection.retrieve_all(facet=('color', 'green'))
    assert choice == [x.name for x in r]


def test_add_relationship(test_db):
    """
    Make sure we can add relationships between collections which are symmetrical
    """
    test_db.relationship.add_single('dummy1', 'dummy3', 'brother')
    x = test_db.relationship.retrieve('dummy1','brother')
    assert 'dummy1' == x[0].subject.name
    x = test_db.relationship.retrieve('dummy3', 'brother')
    assert len(x) == 0

def test_add_relationships(test_db):
    """
    Make sure we can add and use assymetric relationships between collections
    """
    test_db.relationship.add_double('dummy1', 'dummy3', 'parent_of', 'child_of')
    x = test_db.relationship.retrieve('dummy1', 'parent_of')

    assert [('dummy1','dummy3'),('dummy1','dummy3')] == [(j.subject.name,j.related_to.name) for j in x]
    x = test_db.relationship.retrieve('dummy3', 'child_of')
    assert ['dummy3'] == [j.subject.name for j in x]

def test_locations(test_db):
    """
    Test we can see the locations known to the DB
    """
    locs = [l.name for l in test_db.location.all()]
    assert locs == ['testing',]
    loc = 'testing'
    # not sure how many files we have in the test db
    files = test_db.file.in_location(loc)
    # we set all our dummy files up with 10
    l = test_db.location.retrieve(loc)
    assert l.name == loc
    assert l.volume == len(files)*10

def test_delete_files(test_db):
    files = test_db.file.all()
    for f in files:
        f.delete()
    assert test_db.file.count() == 0


def NOtest_delete_file_from_collection():
    raise NotImplementedError



def NOtest_delete_location():
    raise NotImplementedError

def NOtest_deleting_tags():
    pass
       
def NOtest_save_as_collection():
    pass

def NOtest_retrieve_files_by_name():
    pass

def NOtest_retrieve_file_if_present():
    pass

def NOtest_retrieve_files_which_match():
    pass

def NOtest_retrieve_or_make_file():
    pass
    
def NOtest_delete_file_from_variables():
    pass
    
def NOtest_retrieve_files_from_variables():
    pass
    
def NOtest_directory_stuff():
    pass

def NOtest_add_cell_method(test_db):
    # also need to do other cell_method stuff
    pass

def NOtest_add_variable_from_file(test_db):
    pass

