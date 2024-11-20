# read the canari test data and create some smaller files for tests

import cf

def cutdown(flist, fields=['eastward_wind','northward_wind'],years=(1950,1960)):
    """ Givene as set of fields cut them down to the fields and years"""

    results = []
    for field in fields:
        r = flist.select_by_identity(field)
        r2 = [z.subspace(T=cf.year(cf.wi(*years))) for z in r]
        results.append(r2)
    x = results.pop()
    for y in results:
        x+=y
    return x

def handle(infile, outfile):

    flist = cf.read(infile, chunks=None)
    fout = cutdown(flist)
    cf.write(fout, outfile, cfa=True)

if __name__=="__main__":
    from pathlib import Path
    here = Path(__file__).parent.resolve()
    handle(here/'CANARI_1_cs125_atmos.cfa', here/'CANARI_test1.cfa')
    handle(here/'CANARI_26_db305_atmos.cfa', here/'CANARI_test2.cfa')

