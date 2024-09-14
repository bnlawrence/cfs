from core.db.file_handling import cfupload_ncfiles
from pathlib import Path

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
        self.db = db
        self.location = location
        try:
            loc = self.db.location_retrieve(location)
            print(f"Using existing location ({loc})")
        except ValueError:
            loc = self.db.location_create(location)

    def add_collection(
        self,
        path_to_collection_head,
        collection_head_name,
        collection_head_description,
        subcollections=False,
        checksum=None,
        regex=None,
    ):
        """
        Add a new collection with all netcdf files below <path_to_collection_head>,
        and call that collection <collection_head_name>, and decorate with <collection_head_description> text.

        Optionally (<subcollections=True>), create sub-collections for all internal directories
        (default = False = do not create sub-collections). 

        If checksums required, provide a checksum method string.
        (NOT YET IMPLEMENTED) Not Implemented

        """
        try:
            c = self.db.collection_retrieve(collection_head_name)
            raise ValueError(f'Cannot add {collection_head_name} - it already exists')
        except:
            c = self.db.collection_create(collection_head_name, collection_head_description)
        args = [
            path_to_collection_head,
            collection_head_name,
            collection_head_description,
            subcollections,
            checksum,
            regex,
        ]
        keys = [
            "_path_to_collection_head",
            "_collection_head_name",
            "_collection_head_description",
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
                    parents = get_parent_paths(p,basedir,collection_head_name)
                else:
                    parents = []
                dbfiles.append(file2dict(p, parents, checksum=checksum))
                
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

        msg = cfupload_ncfiles(self.db, self.location, c, dbfiles, cfa=cfa)
        print(msg)
     

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