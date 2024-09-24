from pathlib import Path
here = Path(__file__).parent.resolve()
dbfile = here/'canari_test.db'
migrations_location = str(here/'migrations')
from core.db.standalone import setup_django
setup_django(db_file=dbfile, migrations_location=migrations_location)
from core.plugins.posix import Posix
from core.db.interface import CollectionDB

db = CollectionDB()

fests = db.manifest.all()
for f in fests: 
    print(f)

for fragment in fests[0].fragments.all():
    print(fragment)

vars = db.variable.all()

for v in vars:
    print(v.dump())
    print (v.in_manifest.fragments.all()[0])