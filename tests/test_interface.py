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
    Get ourselves a db to work with. Note that this database is progressively
    modified by all the tests that follow. So if you're debugging tests, you 
    have to work though them consecutively.
    """
    tmp_path = tmp_path_factory.mktemp('testing_interface')
    dbfile = str(Path(tmp_path)/'test.db')
    migrations_location = str(Path(tmp_path)/'migrations')
    setup_django(db_file=dbfile,  migrations_location=migrations_location)

@pytest.fixture
def test_db():
    """ 
    This database (and it's contents) is used in all the following
    tests, and is progressively modified as the tests proceed.
    """
    from core.db.interface import CollectionDB
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
    assert 'DB IntegrityError' in str(context)

def test_fileupload(test_db):
    """ Test uploading files """
    test_db.create_collection('mrun3', 'no real description', {})
    test_db.create_location('testing')
    files = [{'path': '/somewhere/in/unix_land', 'name': f'filet{i}', 'size': 10} for i in range(10)]
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
    assert 'Invalid request' in str(context)

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
    files = test_db.retrieve_files_in_location('testing')

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

def test_delete_collection(test_db):
    """
    Make sure delete collection works and respects files in collection
    """
    with pytest.raises(PermissionError) as context:
        test_db.delete_collection('fdummy1')
    files = test_db.retrieve_files_in_collection('fdummy1')
    for f in files:
        test_db.remove_file_from_collection('fdummy1', f.path, f.name)
    test_db.delete_collection('fdummy1')
    with pytest.raises(ValueError) as context:
        c = test_db.retrieve_collection('fdummy1')

def test_remove_from_collection(test_db):
    """
    Test removing file from a collection
    """
    path = '/somewhere/in/unix_land'
    files = test_db.retrieve_files_in_collection('fdummy2')
    # first let's make sure the right thing happens if the file doesn't exist
    with pytest.raises(FileNotFoundError):
        test_db.remove_file_from_collection('fdummy2', path, 'abc123')
    # if it isn't in the collection
    with pytest.raises(ValueError):
        test_db.remove_file_from_collection('fdummy2', path, 'file33')
    files = test_db.retrieve_files_in_collection('fdummy2')
    for f in files:
        test_db.remove_file_from_collection('fdummy2', f.path, f.name)
        # this checks it's no longer in the collection
        with pytest.raises(ValueError):
            test_db.remove_file_from_collection('dummy2', f.path, f.name)
    # and this checks it still exists
    for f in files:
        f = test_db.retrieve_file(f.path, f.name)

def test_retrieve_file(test_db):
    """
    Test retrieving files
    """
    path = '/somewhere/in/unix_land'
    # first let's make sure the right thing happens if the file doesn't exist
    with pytest.raises(FileNotFoundError) as context:
        f = test_db.retrieve_file(path, 'abc123')
    # now check we can find a particular file
    f = test_db.retrieve_file(path, 'file01')
    assert f.name, 'file01'

def test_file_replicants(test_db):
    """
    Test what happens when we add files which have common characteristics in two different locations.
    What we want to happen is that the files appear as one file, with two different replicants.
    We also want to be able to find such files, so we test that here too.
    """
    _dummy(test_db, location='pseudo tape', collection_stem="tdummy", files_per_collection=3)
    # now we need to see if these can be found, let's just look for the two replicas in dummy1
    fset = test_db.retrieve_files_in_collection('tdummy1', replicants=True)
    assert len(fset) == 3
    assert fset[0].name == 'file01'
    # now that set should all be replicated in two locations, let's make sure we don't
    # get them back if we only want the ones in this location.
    fset =  test_db.retrieve_files_in_collection('tdummy1', replicants=False)
    assert len(fset) == 0

def test_file_match_and_replicants(test_db):
    """ 
    Test that we get the same results as file_replicants, but when we use
    a match as well 
    """
    fset = test_db.retrieve_files_in_collection('tdummy2', match='22', replicants=True)
    assert len(fset) == 1
    fset = test_db.retrieve_files_in_collection('tdummy1', replicants=False, match='file2')
    assert len(fset) == 0 # of the four it would be without the match!

def test_locations(test_db):
    """
    Test we can see the locations known to the DB
    """
    locs = [l.name for l in test_db.retrieve_locations()]
    assert locs == ['testing','pseudo tape']
    loc = 'testing'
    # not sure how many files we have in the test db
    files = test_db.retrieve_files_in_location(loc)
    # we set all our dummy files up with 10
    l = test_db.retrieve_location(loc)
    assert l.volume == len(files)*10

def test_new_location(test_db):
    """ Test adding a new location with two protocols"""
    protocols = ['posix','s3']
    newloc = 'New-Location'
    test_db.create_location(newloc,protocols=protocols)
    loc = test_db.retrieve_location(newloc)
    rp = loc.protocolset
    assert protocols == [p.name for p in rp]

def test_new_protocol(test_db):
    """ 
    Test adding a new protocol against a new location and an existing location 
    """
    locations = test_db.retrieve_locations()
    eloc = locations[0].name
    newloc = 'New-Location'
    newp = 'magic'
    test_db.add_protocol(newp, locations=[eloc,newloc])
    for loc in [newloc, eloc]:
        dloc = test_db.retrieve_location(loc)
        locp = [p.name for p in dloc.protocolset]
        assert newp in locp

def test_locate_replicants():
    raise NotImplementedError

def test_delete_file_from_collection():
    raise NotImplementedError

def test_delete_collection():
    raise NotImplementedError

def test_delete_location():
    raise NotImplementedError

def test_deleting_tags():
    pass
       
def test_save_as_collection():
    pass

def test_retrieve_files_by_name():
    pass

def test_retrieve_file_if_present():
    pass

def test_retrieve_files_which_match():
    pass

def test_retrieve_or_make_file():
    pass
    
def test_delete_file_from_variables():
    pass
    
def test_retrieve_files_from_variables():
    pass
    
def test_directory_stuff():
    pass

def test_add_cell_method(test_db):
    # also need to do other cell_method stuff
    pass

def test_add_variable_from_file(test_db):
    pass

