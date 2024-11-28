from pathlib import Path
from time import time
here = Path(__file__).parent.resolve()
import os
os.environ['CFS_DBDIR'] = str(here) 
from django import setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web.settings')
setup()
from django.core.exceptions import ObjectDoesNotExist
from cfs.db.time_handling import check_for_canari_metadata_issues
from cfs.plugins.posix import Posix
##setup_django(db_file=dbfile, migrations_location=migrations_location)
from cfs.db.interface import CollectionDB

db = CollectionDB()

def load(P, cname, restart=True):
    if restart:
        try:
            existing = db.collection.retrieve(name=cname)
            db.collection.delete(existing.name, force=True)
        except ObjectDoesNotExist as e:
            print(e)
        except:
            raise
    t0 = time()
    P.add_collection(str(here),
                cname,
                 "Test CANARI atmospheric aggregation files",
                 regex='CANARI*.cfa',
                 intent='C',
                 fixer = check_for_canari_metadata_issues,
                 vocab='CANARI',
    )
    t1 = time()-t0
    print(f'Canari load took {t1:.2f}s')

def show_vars():

    vars = db.variable.all() 
    for v in vars:
        print(v.dump())

if __name__=="__main__":
    # If we need to blow the database away, remove it from here, and follow the django startup from the docs
    
    P = Posix(db,'cfs data dir')
    load(P, 'canari_test1')
    show_vars()

                 


