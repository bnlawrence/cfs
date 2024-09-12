from time import time
import cf
from core.db.cfparsing import parse_fields_todict
from core.db.models import File, Location, Collection

def cfupload_variables(db, file, collection, extra_collections):
    """
    Parse a file and load cf metadata into the database.
    This version does not handle aggregations. Deliberately.
    : db : a CollectionDB instance
    : file : a db file instance
    : collection : a db collection name
    : extra_collections : names of any extra collections in which these files and variables might appear
    : returns : tuple (status message, time taken in seconds)
    """
    if not isinstance(file, File):
        raise ValueError(f'cfupload_file needs a File instance not a {file.__class__}')
    t1 = time()
    fields = cf.read(file.path)
    descriptions = parse_fields_todict(fields)
    cset = [db.collection_retrieve(c) for c in extra_collections]
    for d in descriptions:
        v = db.variable_retrieve_or_make(d)
        db.variable_add_to_file_and_collection(v, file, collection)
        for c in cset:
             db.variable_add_to_collection(c, v)
    t2 = time()-t1
    return f'cfupload_file: {len(descriptions)} uploaded in {t2:.2f}s',len(descriptions),t2


def cfupload_ncfiles(db, location_name, base_collection_name, dbfiles):
    """ 
    Upload the cf information held in a bunch of files described by "normal file dictionaries"
    with a list of extra target collections embedded in each.
    """

    msgs = []
    t = 0
    nv = 0
    nf = 0 
    t1 = time()
    for fd in dbfiles:
        try:
            collections = fd.pop('collections')
        except KeyError:
            collections = []
        print(fd)
        file = db.upload_file_to_collection(location_name, base_collection_name, fd, lazy=1, update=False)
        m, n, t = cfupload_variables(db, file, base_collection_name, collections)
        msgs.append(m)
        nv += n
        t += n
        nf += 1
    t2 = time()-t1
    return f'cfupload_ncfiles uploaded {nf} files ({nv} variables) which took {t2:.2}({t:.2})s'


   