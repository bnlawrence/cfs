import cf
from time import time

def filelist2cfa(filelist, cfa_file):
    """ 
    The routine takes a filelist produced by the atomic_cmip routine 
    and writes a CFA file for all the contents.
    """
    
    with open(filelist,'r') as f:
        files = [x.strip() for x in f.readlines()]
    t1=time()
    fields = cf.read(files)
    print(fields)
    t2=time()
    print(f'Read {len(fields)} fields in {t2-t1:.2f}s')
    fewer_fields = cf.aggregate(fields)
    t3=time()
    print(f'Aggregation to {len(fewer_fields)} in {t3-t2:.2f}s')
    cf.write(fewer_fields,cfa_file, cfa={'absolute_paths':False,
                               'substitutions':{'base':'./'}})
    t4=time()
    print(f'Writing cfa file took {t4-t3:.2f}s')
    
if __name__=="__main__":
    import sys
    flist = sys.argv[1]
    ofile = sys.argv[2]
    filelist2cfa(flist, ofile)