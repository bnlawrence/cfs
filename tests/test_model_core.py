from pathlib import Path
from core.db.standalone import setup_django
from django.db.models import Count,OuterRef, Subquery
import pytest

def test_unique_files_and_empty(tmp_path):
    """ 
    Test the pieces that we use to build up a unique file count
    and then test that it all works for emptying and deleting
    a collection
    """

    dbfile = str(Path(tmp_path)/'test.db')
    migrations_location = str(Path(tmp_path)/'migrations')

    setup_django(db_file=dbfile,
                 migrations_location=migrations_location)

    from core.db.models import File, Collection

    def make_c(name, files):
        """ Make a collection with name and attach files"""
        kw = {'volume':0, 'description':'no description','batch':1,'_proxied':{}}
        c = Collection(name=name, **kw)
        c.save()
        for f in files: 
            c.files.add(f)
            c.volume +=f.size
        c.save()
        return c
    
    file1 = File(name='myfile1',path='/tmp/myfile1',size=1)
    file2 = File(name='myfile2',path='/tmp/myfile2',size=1)
    file3 = File(name='myfile3',path='/tmp/myfile2',size=1)
    file1.save()
    file2.save()
    file3.save()
    c1 = make_c('tcol1',[file1,file2])
    c2 = make_c('tcol2',[file2,file3])
    #print(c1)
    #print(c2)

    #### The following logic appears inside collection.unique_files()
    #### Ripped out here to fully understand what is going on.

    # this doesn't work because django has been too clever
    # files1 = c1f.annotate(collection_count=Count('collection'))
    # files2 = c2f.annotate(collection_count=Count('collection'))
    
    collection_count_subquery = Collection.objects.filter(
        files=OuterRef('pk')).values('files').annotate(count=Count('id')).values('count')
    

    c1fa = c1.files.annotate(collection_count=Subquery(collection_count_subquery))
    c2fa = c2.files.annotate(collection_count=Subquery(collection_count_subquery))

    #print('a c1',[(f.name, f.collection_count) for f in c1fa])
    #print('a c2',[(f.name,f.collection_count) for f in c2fa])


    #### The rest of the logic here tests that usage within the do_empty
    #### method (and checks we can't delete a non-empty collection) and 
    #### various aspects of the logic of file removal.

    with pytest.raises(PermissionError) as context:
        c1.do_empty()
    assert "Cannot empty tcol1 (contains 1 unique files)" in str(context)

    # no deletion with files 
    with pytest.raises(PermissionError) as context:
        c2.delete()
    assert "Cannot delete" in str(context)

    ### Now check we can't remove a file from a collection if it is the
    ### the last collection it's in. At this point myfile1 is the 
    ### unique file in c1.

    with pytest.raises(PermissionError) as context:
        c1.files.remove(file1)
    
    #### Now see if we can do some magic with do_empty, by testing
    #### each of the four options.

    #option 4 expected to fail
    with pytest.raises(PermissionError) as context:
        c1.do_empty(delete_files=False, unique_only=False) 

    # option 1 to fail 
    with pytest.raises(PermissionError) as context:
        c1.do_empty(delete_files=True, unique_only=True)
    # now remove the file which is in the other collection
    # option 1 to succeed
    c1.files.remove(file2)
    c1.do_empty(delete_files=True)
    c1.delete()

    collections = Collection.objects.all()
    assert 'tcol1' not in [c.name for c in collections]

    c1 = make_c('tcol1',[file2])
    #print('b c1',[f.name for f in c1.files.all()])
    #print('b c2',[f.name for f in c2.files.all()])

    #option 3 to fail
    with pytest.raises(PermissionError) as context:
        c2.do_empty(delete_files=False,unique_only=True)
    #option 3 to succeed 
    c1.do_empty(delete_files=False,unique_only=True)

    #option 2 cannot fail
    c2.do_empty(delete_files=True,unique_only=False)

    