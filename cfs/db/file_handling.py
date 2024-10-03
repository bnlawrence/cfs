from time import time
import logging
logger = logging.getLogger(__name__)
import cf

from cfs.db.cfparsing import parse_fields_todict
from cfs.db.interface import LocationInterface

def cfupload_variables(db, location, fields, fd, collection, extra_collections, cfa=False):
    """
    Parse a set of cf fields and load cf metadata into the database.
    : db : a CollectionDB instance
    : location :  a location name
    : fields : a list of CF field constructs 
    : fd : a file data dictionary
    : collection : a db collection name
    : extra_collections : names of any extra collections in which these files and variables might appear
    : returns : tuple (status message, time taken in seconds)
    """
    
    t2 = time()
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
    logger.info(f'cfupload_file: {len(descriptions)} uploaded in {t3-t2:.2f}s')


def cfupload_ncfiles(db, location_name, base_collection, dbfiles, intent, cfa=False, accessor=None, fixer=None):
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
    : fixer : often the fields data may need to be fixed before uploading.
        If this is necessary, pass a function which can be applied to the list of fields and the name of
        the file the fields were in.

    """
    if intent == 'F':
        raise ValueError('Intent cannot be to be a fragment')
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
        logger.info(f'Handling {fd}')
        xt1 = time()
        fields = cf.read(fd['path'])
        xt2 = time()
        if fixer is not None:
            for f in fields:
                fixer(f, fd['path'])
        xt3 = time()-xt2
        logger.info(f"Initial CF read of {fd['name']} took {xt2-xt1:.2f}s (fixer={xt3:.2f}s)")
        cfupload_variables(db, location_name, fields, fd, base_collection.name, collections, cfa=cfa)
        nv += len(fields)
        nf += 1
    t2 = time()-t1
    msg = f'cfupload_ncfiles uploaded {nf} files ({nv} variables) which took {t2:.2f}s'
    logger.info(msg)

