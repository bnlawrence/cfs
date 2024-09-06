from core.db.standalone import setup_django
from pathlib import Path

import pytest


def _dummy(db, location='testing', collection_stem="fdummy", files_per_collection=10):
    """ 
    Set up a dummy dataset in db with accessible structure for testing
    """
    db.create_location(location)
    for i in range(5):
        c = f'{collection_stem}{i}'
        db.create_collection(c, 'no description', {})
        files = [{'path': '/somewhere/in/unix_land', 'name': f'file{j}{i}', 'size': 10} for j in range(files_per_collection)]
        db.upload_files_to_collection(location, c, files)

@pytest.fixture(scope="session", autouse=True)
def test_db(tmp_path_factory):
    """ 
    Get ourselves a db to work with
    """
    tmp_path = tmp_path_factory.mktemp('testing_interface')
    dbfile = str(Path(tmp_path)/'test.db')
    migrations_location = str(Path(tmp_path)/'migrations')
    setup_django(db_file=dbfile,  migrations_location=migrations_location)

@pytest.fixture
def test_db():
    from core.db.interface import CollectionDB, CollectionError
    return CollectionDB()
   
def test_create_collection(test_db):
    kw = {'resolution': 'N512', 'workspace': 'testws'}
    test_db.create_collection('mrun1', 'no real description', kw)
    info = test_db.collection_info('mrun1')
    print(info)

def test_unique_collection(test_db):
    kw = {'resolution': 'N512', 'workspace': 'testws'}
    test_db.create_collection('mrun2', 'no real description', kw)
    with pytest.raises(ValueError) as context:
        test_db.create_collection('mrun2', 'no real description', kw)
        assert 'Cannot add duplicate collection' in str(context)

def test_fileupload(test_db):
    """ Test uploading files """
    test_db.create_collection('mrun3', 'no real description', {})
    test_db.create_location('testing')
    files = [{'path': '/somewhere/in/unix_land', 'name': f'filet{i}', 'size': 0} for i in range(10)]
    test_db.upload_files_to_collection('testing', 'mrun3', files)
    assert len(test_db.retrieve_files_in_collection('mrun3')) == len(files)

def test_add_and_retrieve_tag(test_db):
    """
    Need to add tags, and select by tags
    """
    tagname = 'test_tag'
    test_db.create_tag(tagname)
    test_db.tag_collection('mrun1', tagname)
    test_db.tag_collection('mrun3', tagname)
    tagged = test_db.retrieve_collections(tagname=tagname)
    assert ['mrun1', 'mrun3'] == [x.name for x in tagged]

def test_get_collections(test_db):
    """
    Test ability to get a subset of collections via name and/or description
    """
    for i in range(5):
        test_db.create_collection(f'dummy{i}','no description', {})
        test_db.create_collection(f'eg{i}','no description', {})
    test_db.create_collection('dummy11','actual description',{})
    assert len(test_db.retrieve_collections(name_contains='g')) == 5
    assert len(test_db.retrieve_collections(description_contains='actual')) ==  1
    with pytest.raises(ValueError) as context:
        test_db.retrieve_collections(description_contains='actual', name_contains='x')
        assert 'Invalid Request' in str(context)

def test_get_collection_fails(test_db):
    """
    Make sure we handle a request for a non-existent collection gracefully
    """
    # expect an empty set, not an error for this one:
    cset = test_db.retrieve_collections(name_contains='Fred')
    assert len(cset) == 0
    with pytest.raises(ValueError) as context:
        fset = test_db.retrieve_files_in_collection('Fred')
        assert 'No such collection' in str(context)
    with pytest.raises(ValueError) as context:
        c = test_db.retrieve_collection('Fred')
        assert 'No such collection' in str(context)

def test_collection_properties(test_db):
    """
    Test ability to create properties on collections and query against them.
    """
    choice = ['dummy2', 'dummy3']
    for c in choice:
        cc = test_db.retrieve_collection(c)
        cc['color'] = 'green'
        cc.save()
    r = test_db.retrieve_collections(facet=('color', 'green'))
    assert choice == [x.name for x in r]


def test_get_files_match(test_db):
    """
    Make sure we can get files in a collection AND
    those in a collection that match a specfic string
    """
    _dummy(test_db)
    files = test_db.retrieve_files_in_collection('fdummy3')
    assert len(files) == 10
    files = test_db.retrieve_files_in_collection('fdummy3', 'file13')
    assert len(files) == 1

def test_add_relationship(test_db):
    """
    Make sure we can add relationships between collections which are symmetrical
    """
    test_db.add_relationship('dummy1', 'dummy3', 'brother')
    x = test_db.retrieve_relationships('dummy1','brother')
    assert 'dummy1' == x[0].subject.name
    x = test_db.retrieve_relationships('dummy3', 'brother')
    assert len(x) == 0

def test_add_relationships(test_db):
    """
    Make sure we can add and use assymetric relationships between collections
    """
    test_db.add_relationships('dummy1', 'dummy3', 'parent_of', 'child_of')
    x = test_db.retrieve_relationships('dummy1', 'parent_of')
    assert ['dummy1'] == [j.subject.name for j in x]
    x = test_db.retrieve_relationships('dummy3', 'child_of')
    assert ['dummy3', ] == [j.subject.name for j in x]

