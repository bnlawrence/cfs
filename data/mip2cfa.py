import cf
from time import time
from pathlib import Path

def filelist2cfa(filelist, extension='.cfa'):
    """ 
    The routine takes a filelist produced by the atomic_cmip routine 
    and writes a CFA file for all the contents. We write the CFA
    back into the same directory used for the text files using the
    same name, but a different extension.
    """
    
    with open(filelist,'r') as f:
        files = [x.split(',')[0].strip()  for x in f.readlines()]
    t1=time()
    # ideally we wouldn't do the chunks=None, see https://github.com/bnlawrence/cfs/issues/19
    fields = cf.read(files, chunks=None)
    print(fields)
    t2=time()
    print(f'Read {len(fields)} fields in {t2-t1:.2f}s')
    fewer_fields = cf.aggregate(fields)
    t3=time()
    print(f'Aggregation to {len(fewer_fields)} in {t3-t2:.2f}s')

    if not isinstance(filelist, Path):
        filelist = Path(filelist)
    cfa_file = filelist.with_suffix(extension)

    cf.write(fewer_fields,cfa_file, cfa={'absolute_paths':False,
                               'substitutions':{'base':'./'}})
    t4=time()
    print(f'Writing cfa file took {t4-t3:.2f}s')
    
if __name__=="__main__": 
    import sys
    flist = sys.argv[1]
    filelist2cfa(flist)