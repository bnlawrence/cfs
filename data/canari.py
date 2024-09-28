from pathlib import Path
from time import time
here = Path(__file__).parent.resolve()
import os
from django import setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web.settings')
setup()


from cfs.plugins.posix import Posix
##setup_django(db_file=dbfile, migrations_location=migrations_location)
from cfs.db.interface import CollectionDB

db = CollectionDB()

def load(P, cname, restart=True):
    if restart:
        try:
            existing = db.collection.retrieve_by_name(cname)
            if existing:
                db.collection.delete(existing.name, force=True)
        except ValueError:
            pass
    t0 = time()
    P.add_collection(str(here),
                cname,
                 "One of the CANARI atmospheric aggregation files",
                 regex='*.cfa',
                 intent='C'
    )
    t1 = time()-t0
    print(f'Canari load took {t1:.2f}s')

def show_vars():

    vars = db.variable.all()
    for v in vars:
        print(v.dump())
        print (v.in_manifest.fragments.all()[0])

if __name__=="__main__":
    P = Posix(db,'cfs data dir')
    load(P, 'canari_test1')
    show_vars()

                 


