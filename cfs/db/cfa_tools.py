from pathlib import Path
import h5netcdf as h5
import numpy as np
import uuid, io
from time import time
import hashlib
import logging
import cf


logger = logging.getLogger(__name__)
logger.setLevel('WARN')

def bounds_range(bounds_array):
    """ Convenience tool for looking at the bounds
    of a cf field"""
    x = bounds_array.data[0]
    y = bounds_array.data[-1]
    print('Bounds range ',x,y)
    return x[:,0],y[:,-1]

def numpy2db(an_array):
    """
    Take a numpy array and serialise it for storage in a database
    :param an_array: Numpy array
    :type an_array: np.array type
    :returns: binary blob for storage in database
    """
    binary_stream = io.BytesIO()
    np.save(binary_stream, an_array)
    result = binary_stream.getvalue()
    return result

def db2numpy(blob):
    """
    Take a binary blob stored in the database and return it to a numpy array
    :param blob: blob of data
    :type blob: sequence of bytes produced by numpy2db
    :returns: a numpy array
    """
    binary_stream = io.BytesIO(blob)
    binary_stream.seek(0)
    nparray = np.load(binary_stream)
    return nparray

def consistent_hash(mylist):
    """ 
    Generate a hash that can be reused accross program runs, python's
    internal has includes randomisation
    """
    tuple_list=tuple(mylist)
    # Convert the list of tuples to a string representation
    data = str(tuple_list).encode('utf-8')
    return hashlib.md5(data).hexdigest()

def cf_cells_overlap(value0, value1, units=None):
    """ Will be in next CF release"""
    #FIXME: When in general release, remove 
    return cf.Query("ge", value0, units=units, attr="upper_bounds") & cf.Query(
        "le", value1, units=units, attr="lower_bounds"
    )

def get_quark_field(instance, start_date, end_date):
    """ Given a CFS manifest field (i.e. an instance of the Manifest class 
    found in the django models.py) and a pair of bounding dates, return
    a CF field instance with the information needed to construct a 
    subspace manifest.
    : instance : A CFS manifest instance
    : start_date : A date in a cf.Data instance
    : end_date : A date in a cf.Data instance.
    : output : A CF field instance with the correct bounds, and 
    a set of fragment ids in the field data.
    """

    fld = cf.Field(properties={'long_name':'fragments'})
    
    fragments = np.array([f.id for f in instance.fragments.files.all()])
    T_axis= fld.set_construct(cf.DomainAxis(fragments.size))
    fld.set_data(fragments, axes=T_axis)

    bounds = db2numpy(instance.bounds)
   
    timedata = np.mean(bounds, axis=1)
    
    dimT = cf.DimensionCoordinate(properties={'standard_name':'time',
                    'units':cf.Units(instance.units,calendar=instance.calendar)},
                data = timedata,
                bounds = cf.Bounds(data=bounds)
                )
    left, right = bounds_range(dimT.bounds)
    if start_date < left or end_date > right:
        raise ValueError(f'Cannot find a quark manifest: Dates {start_date},{end_date} not within {left}{right}')
    
    nf, nt = len(fragments), len(timedata)
    logging.debug(f'Creating time dimension for {nf} fragments with {nt} time entries')

    if nf != nt:
      
        raise RuntimeError(
            f'Number of of manifest fragments ({nf}) not equal to time data length ({nt}).')
   
    fld.set_construct(dimT, axes=T_axis)
    print('Subspacing using cellwi to ',start_date, end_date)

    print(fld.dump())
    quark = fld.subspace(time=cf_cells_overlap(start_date, end_date))
    dimT = quark.dimension_coordinate('T')
    return quark, bounds_range(dimT.bounds)



class CFAManifest:
    """ 
    Used to work with manifests, outside of the database
    """
    # cfa fragment files are held as dictionary items with keys
    fragment_template = {'name':None,'path':None,'size':None,'type':'F'}
    
    def __init__(self,uuid=None, accessor=None):
        """ 
        Initialise with the tracking_id of the parent CFA file if
        available and a tool for getting fragment file size, if available.
        : uuid : parent file tracking id
        : accessor : Class instance of tool that can do this.
                     e.g. PosixAccessor from the posix.py in plugins
        """
        self.bounds = None 
        self._bounds_ncvar = None 
        self.units = None
        self.calendar = None
        self.fragments = {}
        self.accessor = accessor
        self.parent_uuid = uuid
        self.manikey = None

    def add_fragment(self, file_path):
        """ 
        Add fragment via file_path.
        For the moment I'm not handling multiple fragments. 
        """
        if file_path in self.fragments:
            raise ValueError('Attempt to add existing fragment into manifest')
        fragment = self.fragment_template.copy()
        name =  Path(file_path).name
        if ':' in name:
            base, name = name.split(':')
            if base != '':
                fragment['base'] = base
        fragment['name'] = name
        fragment['path'] = file_path
        if self.accessor:
            fragment['size'] = self.accessor.get_size(file_path)
        self.fragments[file_path] = fragment
    def get_dbdict(self):
        """ 
        Return a dictionary suitable for uploading
        """
        self.manikey = consistent_hash(self.fragments.keys())
        mydict = {k:getattr(self, k) for k in ['manikey','_bounds_ncvar','fragments','parent_uuid','units','calendar','bounds',]}
        return mydict
    def add_bounds(self, bounds, units, calendar, ncvar):
        """ 
        Add the (temporal) bounds of the fragments
        """
        self.units = units
        self.calendar = calendar
        self._bounds_ncvar = ncvar
        self.bounds = numpy2db(bounds)


    @classmethod
    def from_dbdict(cls, dbdict):
        """
        Create a CFAManifest instance from a dictionary.
        Bounds are intentionally not included in the recreation.
        : dbdict : The dictionary produced by get_dbdict
        """
        # we don't need an accessor because we've got the fragment info
        # Initialize the new instance with the uuid and accessor
        instance = cls(uuid=dbdict.get('uuid'))

        # Populate the fragments
        instance.fragments = dbdict.get('fragments', {})

        # Bounds are intentionally left out
        return instance

class CFAhandler:
    """" 
    The CFA handler is used during the parsing of a CFA file to 
    construct one or more manifests corresponding to the 
    fragment file sets discovered in the CFA file.
    As the fields are parsed, the manifests are constructed,
    and for each field, a "manikey" identifier is provided
    so that these fields can be matched to the manifests
    stored in the known manifest directory.
    """
    def __init__(self, expected_fields, accessor=None):
        """ Opens the aggregation file for further parsing.
        We need to cover the situation where the variables of the 
        aggregation use different fragment sets with differing bounds.
        We want to avoid file and variable handling if we can.
        : expected_fields : total number of fields in file 
        """
        self.dataset = None
        self.known_manifests = {}
        self.expected = expected_fields
        self.parsed = 0
        self.known_coordinates = {}
        self.accessor = accessor

    def parse_field_to_manifest(self, field):
        """ 
        This is the entry point for matching a field
        to an existing manifest or creating one if this
        is necessary.
        : field : individual CF field  from CFA File
        : accessor : tool used for getting file sizes etc
        """
        #FIXME: Add tracking id
        #FIXME: All this file handling is brittle. Help David!
        self.arrived_at = time()
        # at the moment we need to do it from the field, so we do get the CFA file
        # in the future, we can maybe use field.data.get_filenames since we 
        # have access to this filename via other routes.
        files =  field.get_filenames()
        if self.dataset is None:
            cfa_file = [f for f in files if Path(f).suffix == '.cfa'][0]
            self.dataset = h5.File(cfa_file,'r')

        # we can only do the sorting because we have timestamps. CF
        # uses a set because aggregation could be multidimensionsonal
        filenames = sorted([f for f in files if Path(f).suffix !='.cfa'])
        tdim = field.dimension_coordinate('T', default=None)
        if tdim is not None:
            tdimvar = tdim.nc_get_variable()
        else:
            tdimvar = None

        calendar = getattr(tdim, 'calendar', 'standard')

        #have we seen this before?
        manikey = consistent_hash(filenames)
        if manikey in self.known_manifests:
            # maybe, the filenames match
            candidate_manifest = self.known_manifests[manikey]
            # now check the bounds
            if tdimvar == candidate_manifest['_bounds_ncvar']:
                # same variable used for bounds here and in manifest, it's the same
                logger.info(f'Existing manifest used for {field.identity()} ({self.parsed}/{self.expected})')
                return candidate_manifest['manikey']
            else:
                # make a copy and add new bounds from this field
                new_manifest = CFAManifest.from_dbdict(candidate_manifest)
                if tdimvar is not None:
                    bounds = self._parse_bounds_from_field(field, tdim)
                    new_manifest.add_bounds(bounds, tdim.units, calendar, tdimvar)
                   
                # th other option is that we've got no bounds, the boundless manifest is what we want.
                return self._add_known_and_exit(new_manifest,field)
               
        # ok, carry on, we're constructing it from scratch
        new_manifest = CFAManifest(accessor=self.accessor)
        for f in filenames:
            new_manifest.add_fragment(f)
        if tdim is not None:
            bounds = self._parse_bounds_from_field(field, tdim)
            new_manifest.add_bounds(bounds, tdim.units, calendar, tdimvar)
        return self._add_known_and_exit(new_manifest, field)

    def _add_known_and_exit(self, manifest, field):
        """ 
        Add the manifest to our known manifests and return the appropriate key
        : manifest : A CFA manifest instance 
        : field : the field being processed
        """
        dbdict = manifest.get_dbdict()
        self.known_manifests[dbdict['manikey']]=dbdict
        time_taken = time() - self.arrived_at
        logger.info(f'New manifest constructed for {field.identity()} in {time_taken:.2f}s ({self.parsed}/{self.expected})')
        return dbdict['manikey']

    def _parse_bounds_from_field(self, field, tdim):
        """ 
        Sort out the time bounds corresponding to each fragment file
        """
        ncvar = field.nc_get_variable()
        alocations = self.__get_cfalocation(ncvar)
        if tdim.has_bounds():
            bounds = tdim.bounds.data.array
            #FIXME: Do this directly with numpy
            newbounds = []
            left=0
            for n in alocations:
                right = left + n -1
                newbounds.append([bounds[left][0],bounds[right][1]])
                left+=n
            return np.array(newbounds)
        else:
            raise NotImplementedError

    def __get_cfalocation(self,ncvar):
        """ 
        Find the CFA location by going down the rabbit hole
        : ncvar :  the netcdf variable name of the field of interest
        (We use this to go find the aggregation information and 
        to get the width of each of the fragments in bound space.)
        """
        ncv = self.dataset.variables[ncvar]
        aggregated_data = ncv.attrs['aggregated_data']
        parsed_aggregated_data = dict(zip(aggregated_data.split()[::2], aggregated_data.split()[1::2]))
        location = parsed_aggregated_data['map:']
        #FIXME: this assumes time is the first dimension 
        location = self.dataset.variables[location][:][0]
        return location
