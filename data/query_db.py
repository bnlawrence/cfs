from pathlib import Path
here = Path(__file__).parent.resolve()
import os
os.environ['CFS_DBDIR']=str(here)
from django import setup
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web.settings')
setup()
from cfs.db.interface import CollectionDB

db = CollectionDB()

fests = db.manifest.all()
for f in fests: 
    print(f)

print(fests[0].fragments_as_text())

vars0 = db.variable.all()

vars1 = db.variable.retrieve_by_properties({'standard_name':'eastward_wind'})
vars2 = db.variable.retrieve_by_properties({'frequency':'6hr_pt'})
vars3 = db.variable.retrieve_by_properties({'standard_name':'eastward_wind',
                                                    'frequency':'6hr_pt'})

print(len(vars0), len(vars1), len(vars2), len(vars3))

assert len(vars3) == 6
