from core.db.standalone import setup_django
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
    db.location_create(location)
    for i in range(5):
        c = f'{collection_stem}{i}'
        db.collection_create(c, 'no description', {})
        files = [{'path': '/somewhere/in/unix_land', 'name': f'file{j}{i}', 'size': 10} for j in range(files_per_collection)]
        db.upload_files_to_collection(location, c, files)

@pytest.fixture(scope="session", autouse=True)
def setup_test_db(tmp_path_factory):
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
   
def test_collection_create(test_db):
    kw = {'resolution': 'N512', 'workspace': 'testws'}
    info = test_db.collection_create('mrun1', 'no real description', kw)
    assert info.name == 'mrun1'
    assert info.description == 'no real description'

def test_unique_collection(test_db):
    kw = {'resolution': 'N512', 'workspace': 'testws'}
    test_db.collection_create('mrun2', 'no real description', kw)
    with pytest.raises(ValueError) as context:
        test_db.collection_create('mrun2', 'no real description', kw)
    assert 'DB IntegrityError' in str(context)

def test_fileupload(test_db):
    """ Test uploading files """
    test_db.collection_create('mrun3', 'no real description', {})
    test_db.location_create('testing')
    files = [{'path': '/somewhere/in/unix_land', 'name': f'filet{i}', 'size': 10} for i in range(10)]
    test_db.upload_files_to_collection('testing', 'mrun3', files)
    assert len(test_db.files_retrieve_in_collection('mrun3')) == len(files)
    c = test_db.collection_retrieve('mrun3')
    assert c.volume==100

def test_add_and_retrieve_tag(test_db):
    """
    Need to add tags, and select by tags
    """
    tagname = 'test_tag'
    test_db.tag_create(tagname)
    test_db.tag_collection('mrun1', tagname)
    test_db.tag_collection('mrun3', tagname)
    tagged = test_db.collections_retrieve(tagname=tagname)
    assert ['mrun1', 'mrun3'] == [x.name for x in tagged]

def test_get_collections(test_db):
    """
    Test ability to get a subset of collections via name and/or description
    """
    for i in range(5):
        test_db.collection_create(f'dummy{i}','no description', {})
        test_db.collection_create(f'eg{i}','no description', {})
    test_db.collection_create('dummy11','actual description',{})
    assert len(test_db.collections_retrieve(name_contains='g')) == 5
    assert len(test_db.collections_retrieve(description_contains='actual')) ==  1
    with pytest.raises(ValueError) as context:
        test_db.collections_retrieve(description_contains='actual', name_contains='x')
    assert 'Invalid request' in str(context)

def test_get_collection_fails(test_db):
    """
    Make sure we handle a request for a non-existent collection gracefully
    """
    # expect an empty set, not an error for this one:
    cset = test_db.collections_retrieve(name_contains='Fred')
    assert len(cset) == 0
    with pytest.raises(ValueError) as context:
        fset = test_db.files_retrieve_in_collection('Fred')
    assert 'No such collection' in str(context)
    with pytest.raises(ValueError) as context:
        c = test_db.collection_retrieve('Fred')
    assert 'No such collection' in str(context)

def test_collection_properties(test_db):
    """
    Test ability to create properties on collections and query against them.
    """
    choice = ['dummy2', 'dummy3']
    for c in choice:
        cc = test_db.collection_retrieve(c)
        cc['color'] = 'green'
        cc.save()
    r = test_db.collections_retrieve(facet=('color', 'green'))
    assert choice == [x.name for x in r]


def test_get_files_match(test_db):
    """
    Make sure we can get files in a collection AND
    those in a collection that match a specfic string
    """
    _dummy(test_db)
    files = test_db.files_retrieve_in_collection('fdummy3')
    assert len(files) == 10
    files = test_db.files_retrieve_in_collection('fdummy3', 'file13')
    assert len(files) == 1
    files = test_db.files_retrieve_in_location('testing')

def test_add_relationship(test_db):
    """
    Make sure we can add relationships between collections which are symmetrical
    """
    test_db.relationship_add('dummy1', 'dummy3', 'brother')
    x = test_db.relationships_retrieve('dummy1','brother')
    assert 'dummy1' == x[0].subject.name
    x = test_db.relationships_retrieve('dummy3', 'brother')
    assert len(x) == 0

def test_add_relationships(test_db):
    """
    Make sure we can add and use assymetric relationships between collections
    """
    test_db.relationships_add('dummy1', 'dummy3', 'parent_of', 'child_of')
    x = test_db.relationships_retrieve('dummy1', 'parent_of')
    assert ['dummy1'] == [j.subject.name for j in x]
    x = test_db.relationships_retrieve('dummy3', 'child_of')
    assert ['dummy3', ] == [j.subject.name for j in x]

def test_remove_from_collection(test_db):
    """
    Test removing file from a collection
    """
    test_db.collection_create('dcol1','for file testing')
    test_db.collection_create('dcol2','for file testing')
    dfiles = [{'path': '/bound/for/deletion', 'name': f'remfile{j}', 'size': 10} for j in range(3)]
    test_db.upload_files_to_collection('testing','dcol1',dfiles[0:2])
    # this should only add one new file
    test_db.upload_files_to_collection('testing','dcol2',[dfiles[2]])
    
    files = [test_db.file_retrieve_by_properties(**f) for f in dfiles]
    test_db.file_add_to_collection('dcol2',files[1],skipvar=True)

    # it's unique and can't be removed
    with pytest.raises(PermissionError):
        test_db.file_remove_from_named_collection('dcol1',files[0])
    # it's ok to remove and we can remove it

    assert files[1].collection_set.count()==2
    test_db.file_remove_from_named_collection('dcol1',files[1])
    assert files[1].collection_set.count()==1

    
def test_retrieve_file(test_db):
    """
    Test retrieving files
    """
    path = '/somewhere/in/unix_land'
    # first let's make sure the right thing happens if the file doesn't exist
    with pytest.raises(FileNotFoundError) as context:
        f = test_db.file_retrieve_by_properties(name='abc123')
    # now check we can find a particular file
    f = test_db.file_retrieve_by_properties(name='file01')
    assert f.name, 'file01'

def test_file_replicants(test_db):
    """
    Test what happens when we add files which have common characteristics in two different locations.
    What we want to happen is that the files appear as one file, with two different replicants.
    We also want to be able to find such files, so we test that here too.
    """
    _dummy(test_db, location='pseudo tape', collection_stem="tdummy", files_per_collection=3)
    # now we need to see if these can be found, let's just look for the two replicas in dummy1
    fset = test_db.files_retrieve_in_collection('tdummy1', replicants=True)
    assert len(fset) == 3
    assert fset[0].name == 'file01'
    # now that set should all be replicated in two locations, let's make sure we don't
    # get them back if we only want the ones in this location.
    fset =  test_db.files_retrieve_in_collection('tdummy1', replicants=False)
    assert len(fset) == 0

def test_file_match_and_replicants(test_db):
    """ 
    Test that we get the same results as file_replicants, but when we use
    a match as well 
    """
    fset = test_db.files_retrieve_in_collection('tdummy2', match='22', replicants=True)
    assert len(fset) == 1
    fset = test_db.files_retrieve_in_collection('tdummy1', replicants=False, match='file2')
    assert len(fset) == 0 

def test_locate_files_in_other_collections_as_well(test_db):
    """ 
    At this point there ought to be ten files in fdummy3, 3 of which are duplicated in tdummy3.
    This query tests that we recover from fdummy3 those files which are also in tdummy3
    """
    fset = [f.name for f in test_db.files_retrieve_in_collection('tdummy3')]
    gset = test_db.files_retrieve_in_collection('fdummy3')
    assert len(gset) > len(fset)
    files = [f.name for f in test_db.files_retrieve_in_collection_and_elsewhere('fdummy3',by_properties=False)]
    assert files == fset

def test_delete_collection(test_db):
    """ 
    test deleting a collection with no files, with files only in it, and with
    files which are duplicated. We expect the first to work, the second not
    to work without the correct delete_file argument, and the third to work.
    """
    # This is now fully tested in test_counting
    pass

def test_locations(test_db):
    """
    Test we can see the locations known to the DB
    """
    locs = [l.name for l in test_db.locations_retrieve()]
    assert locs == ['testing','pseudo tape']
    loc = 'testing'
    # not sure how many files we have in the test db
    files = test_db.files_retrieve_in_location(loc)
    # we set all our dummy files up with 10
    l = test_db.location_retrieve(loc)
    assert l.volume == len(files)*10

def test_new_location(test_db):
    """ Test adding a new location with two protocols"""
    protocols = ['posix','s3']
    newloc = 'New-Location'
    test_db.location_create(newloc,protocols=protocols)
    loc = test_db.location_retrieve(newloc)
    rp = loc.protocolset
    assert protocols == [p.name for p in rp]

def test_new_protocol(test_db):
    """ 
    Test adding a new protocol against a new location and an existing location 
    """
    locations = test_db.locations_retrieve()
    eloc = locations[0].name
    newloc = 'New-Location'
    newp = 'magic'
    test_db.protocol_add(newp, locations=[eloc,newloc])
    for loc in [newloc, eloc]:
        dloc = test_db.location_retrieve(loc)
        locp = [p.name for p in dloc.protocolset]
        assert newp in locp


def NOtest_handle_upload_duplicates():

    acol1 = test_db.collection_create('acol1','for file testing')
    acol2 = test_db.collection_create('acol2','for file testing')
    files = [{'path': '/yeah/what', 'name': f'uptestfile{j}', 'size': 10} for j in range(3)]
    test_db.upload_files_to_collection('testing','acol1',files[0:2])
    # this should only add one new fiedl, but it currently adds two
    test_db.upload_files_to_collection('testing','acol2',files[1:3])


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

