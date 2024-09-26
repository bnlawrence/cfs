from pathlib import Path
import sys

here = Path(__file__).parent.resolve()

# Add the parent directory of `cfs` to the Python path
project_root = here.parent
print(project_root)
sys.path.append(str(project_root))

dbfile = here/'cfsdb.sqlite3'
#migrations_location = str(here/'migrations')
#from core.db.standalone import setup_django

from django.conf import settings
from django import setup

from ..core.plugins.posix import Posix
##setup_django(db_file=dbfile, migrations_location=migrations_location)
from core.db.interface import CollectionDB

settings.configure()
setup()
db = CollectionDB()

def load(P, cname, restart=True):
    if restart:
        try:
            existing = db.collection.retrieve_by_name(cname)
            if existing:
                db.collection.delete(existing.name, force=True)
        except ValueError:
            pass
        
    P.add_collection(str(here),
                cname,
                 "One of the CANARI atmospheric aggregation files",
                 regex='*.cfa',
                 intent='C'
    )

def show_vars():

    vars = db.variable.all()
    for v in vars:
        print(v.dump())
        print (v.in_manifest.fragments.all()[0])

if __name__=="__main__":
    P = Posix(db,'cfs data dir')
    load(P, 'canari_test1')
    show_vars()

                 


