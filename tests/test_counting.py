from pathlib import Path
from core.db.standalone import setup_django
from django.db.models import Count,OuterRef, Subquery

def NOtest_blank_database(tmp_path):
    """ Test that the system can install a blank database with our basic model"""

    dbfile = str(Path(tmp_path)/'test.db')
    migrations_location = str(Path(tmp_path)/'migrations')

    setup_django(db_file=dbfile,
                 migrations_location=migrations_location)

    from core.db.models import File, Collection

    kw = {'volume':0, 'description':'no description','batch':1,'_proxied':{}}
    c1 = Collection(name='col1',**kw)
    c2 = Collection(name="col2",**kw)
    c1.save()
    c2.save()
    file1 = File(name='myfile1',path='/tmp/myfile1',size=1)
    file2 = File(name='myfile2',path='/tmp/myfile2',size=1)
    file3 = File(name='myfile3',path='/tmp/myfile2',size=1)
    file1.save()
    file2.save()
    file3.save()
    c1.files.add(file1)
    c1.files.add(file2)
    c2.files.add(file2)
    c2.files.add(file3)
    c1.save()
    c2.save()

    c1f = c1.files
    c2f = c2.files

    # this doesn't work because django has been too clever
    # files1 = c1f.annotate(collection_count=Count('collection'))
    # files2 = c2f.annotate(collection_count=Count('collection'))
    
    collection_count_subquery = Collection.objects.filter(
        files=OuterRef('pk')).values('files').annotate(count=Count('id')).values('count')
    
    c1fa = c1f.annotate(collection_count=Subquery(collection_count_subquery))
    c2fa = c2f.annotate(collection_count=Subquery(collection_count_subquery))

    print('c1',[(f.name, f.collection_count) for f in c1fa])
    print('c2',[(f.name,f.collection_count) for f in c2fa])

    #files = files.filter(collection_count__gt=1).distinct()

    print(file2.collection_set.all())
    