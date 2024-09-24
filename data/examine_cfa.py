import cf 
from pathlib import Path

# this file also online at
#/gws/nopw/j04/canari/users/gobnccas/generatedncfiles/atmos
#
here = Path(__file__).parent.resolve()
file = 'CANARI_1_cs125_atmos.cfa'
egfile = here/file

fields = cf.read(egfile)

hfields = [f for f in fields if getattr(f,'standard_name',None) == 'specific_humidity']

for h in hfields:
    print(h)

for h in hfields:
    h.dump()



