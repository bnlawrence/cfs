from time import time
import logging
logger = logging.getLogger(__name__)
import cf

from cfs.db.cfparsing import parse_fields_todict
from cfs.db.interface import LocationInterface

def cfupload_variables(db, location, fd, collection, extra_collections, cfa=False):
    """
    Parse a file and load cf metadata into the database.
    : db : a CollectionDB instance
    : location :  a location name
    : file : a file description dictionary
    : collection : a db collection name
    : extra_collections : names of any extra collections in which these files and variables might appear
    : returns : tuple (status message, time taken in seconds)
    """
    
    t1 = time()
    fields = cf.read(fd['path'])
    t2 = time()
    logger.info(f"Initial CF read of {fd['name']} took {t2-t1:.2f}s")
    descriptions, manifests = parse_fields_todict(fields, cfa=cfa)
    t2b = time()-t2
    logger.info(f'Parsing to dictionary took {t2b:.2f}s')

    filedata = {'properties':fd, 
                'variables':descriptions}
    if cfa:
        filedata['manifests'] = manifests

    vars = db.upload_file_to_collection(location, collection, filedata)
    for ec in extra_collections:
        for v in vars:
            db.variable.add_to_collection(ec,v)

    t3 = time()
    return f'cfupload_file: {len(descriptions)} uploaded in {t3-t1:.2f}s',len(descriptions),t3-t2


def cfupload_ncfiles(db, location_name, base_collection, dbfiles, intent,  cfa=False, accessor=None):
    """ 
    Upload the cf information held in a bunch of files described by "normal file dictionaries"
    with a list of extra target collections embedded in each.
    : location_name : storage location name
    : base_collection : this will be a collection name used for _this_ set of file uploads.
    : dbfiles : a list of file details
    : intent : the collection intent, which will correspond to the filetype
    : cfa : if the list of files is a list of CFA files
    : accessor : optional class for handling inspection of CFA fragments 
                (What, if anything can be done with the fragment file details will depend
                on the capability provided by this class. If None, then only the 
                path informaiton is used.)

    """
    if intent == 'F':
        raise ValueError('Intent cannot be to be a fragment')
    msgs = []
    t = 0
    nv = 0
    nf = 0 
    t1 = time()
    loci = LocationInterface()
    loc, created = loci.get_or_create(location_name)
    for fd in dbfiles:
        try:
            collections = fd.pop('collections')
        except KeyError:
            collections = []
        fd['type']=intent
        fd['location']=loc
        logger.info('Handling {fd}')
        m, n, t = cfupload_variables(db, location_name, fd, base_collection.name, collections, cfa=cfa)
        msgs.append(m)
        nv += n
        t += n
        nf += 1
    t2 = time()-t1
    msg = f'cfupload_ncfiles uploaded {nf} files ({nv} variables) which took {t2:.2f}({t:.2f})s'
    logger.info(msg)
    return msg

