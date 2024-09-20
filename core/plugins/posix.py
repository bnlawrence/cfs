from core.db.file_handling import cfupload_ncfiles
from pathlib import Path
import logging
logger = logging.getLogger(__name__)

class Posix:
    """
    Supports establishing and/or updating a cfstore view of _part_ or _all_ of a 
    posix storage path, using a "collection_head" for the entire thing, and 
    "sub_collection_of" for all directories within it.
    (Updating not yet implemented)
    """

    def __init__(self, db, location):
        """
        Initialise PosixGWS with database and location name. Will instantiate location
        in database if necessary.
        """
        #print('logging disabled?',logging.root.manager.disable)
        self.db = db
        self.location = location
        try:
            loc = self.db.location_retrieve(location)
            logger.info(f"Using existing location ({loc})")
        except ValueError:
            loc = self.db.location_create(location)
            logger.info(f"Using new location ({loc})")

    def add_collection(
        self,
        path_to_collection_head,
        collection_name,
        collection_description='',
        subcollections=False,
        checksum=None,
        regex='*.nc',
        intent='S'
    ):
        """
        Add a new collection with all netcdf files below a particular path.
        
        : path_to_collection_head : the location in which we will look for files
        : collection_name : this is the collection name to be used in the database, it needs to be uniuque.
        : collection_description : markdown text describing the collection (optinal)
            and call that collection <collection_head_name>, and decorate with <collection_head_description> text.
        : subcollections : boolean, if False, all files (and variables) are added to <collection_name> regardless of
            where they might lie in any directory hierarcy, if present.
            if True, additional collections are created for each sub-directory.
        : checksums : boolean, if checksums are required, provide a string defining the checksum method to be used.
           (checksum support is not yet implemented)
        : regex : by default, we look for nc files, an important other option would be '*.cfa' to look for CFA
            files. However, any valid Pathlib glob string can be used.
        : intent : a single letter which should correspond to the intended type of collection, which should represent
                 whether or not the collection consists of atomic datasets, quarks, or standalone files.
                 (A, Q, S).
        """
        # Require a unique collection name here
        try:
            c = self.db.collection_retrieve(collection_name)
            raise ValueError(f'Cannot add {collection_name} - it already exists')
        except:
            c = self.db.collection_create(collection_name, collection_description)
            self.db.collection_type_add(c,'_type',intent)

        args = [
            path_to_collection_head,
            collection_name,
            collection_description,
            subcollections,
            checksum,
            regex,
        ]
        keys = [
            "_path_to_collection_head",
            "_collection_name",
            "_collection_description",
            "_subcollections",
            "_checksum",
            "_regex",
        ]

        if regex is None:
            regex = '*.nc'
        cfa = regex == '*.cfa'

        if regex not in ['*.nc','*.cfa']:
            raise NotImplementedError


        # record details of how collection was established as collection properties
        for k,v in zip(keys,args):
            c[k]=v
        c.save()
        
        # walk the directory view
        basedir = Path(path_to_collection_head)
        dbfiles = []
        for p in basedir.rglob(regex):
            if p.is_file():
                if subcollections:
                    parents = get_parent_paths(p,basedir,collection_name)
                else:
                    parents = []
                dbfiles.append(file2dict(p, parents, checksum=checksum))
                
        if len(dbfiles) == 0:
            print(f'No {regex} files found at {basedir}')
            return
        # create all the subcollections
        for f in dbfiles:
            collections = f['collections']
            for sc in collections:
                try:
                    cc = self.db.collection_retrieve(str(sc))
                except ValueError:
                    cc = self.db.collection_create(str(sc), description="Subdirectory of collection {c}")
                    pd = str(Path(sc).parent)
                    #print(f'Created {cc} with parent {pd}')
                    #ppd = self.db.collection_retrieve(pd)
                    self.db.relationships_add(pd,cc.name,'parent_of','subdir_of')
        logger.info('Before call')
        msg = cfupload_ncfiles(self.db, self.location, c, dbfiles, intent, cfa=cfa)
        logger.info(msg)
     

def file2dict(p, parents, checksum=None):
    """
    Build the dictionary of file information needed for cfstore.
    The input information is
    : p : full path of file
    : parents : the collections it needs to be added to
    : checksum : checksum option
    """
    if checksum is not None:
        raise NotImplementedError
    f = {"size": p.stat().st_size, "path": str(p), "name":str(p.name), 'collections':parents}

    return f

def get_parent_paths(path, basedir, headname):
    if basedir not in path.parents:
        raise ValueError(f'Base directory {basedir} not part of path {path}')
    parents = []
    for parent in path.parents:
        if parent == basedir:
            break
        relative_path = parent.relative_to(basedir)
        parents.append(f'{headname}/{relative_path}')
    return parents


class PosixAccessor:
    """ 
    This class is used within the fragment handling
    to find the size (and optionally, checksum, of any 
    files which are accessible at fragment ingestion 
    """
    def  __init__(self, checksum_method=None):
        self.checksum_method = checksum_method
        self.known_
    def get(self, path):
        pass
