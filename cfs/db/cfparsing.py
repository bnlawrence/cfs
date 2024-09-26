
import cf
import h5netcdf as h5
import numpy as np
from cfs.db.cfa_tools import CFAhandler
from time import time
import logging
logger = logging.getLogger(__name__)

def manage_types(value):
    """
    For full generality of eventual usage, turn any items into standard Python types.
    """
    if isinstance(value, str):
        return value
    elif isinstance(value, bool):
        return value
    elif isinstance(value, np.int32):
        return int(value)
    elif isinstance(value, int):
        return value
    elif isinstance(value, np.floating):
        return float(value)
    else:
        raise ValueError("Unrecognised type property type", type(value))
    

class LookupXY:
    """ 
    Copy and extend for different grids. You can then pass your lookup table to the 
    parsing routine.I It expects a two-member tuple with Y,Z dimensions.
    """
    def __init__(self, shape):
        self._shape = tuple(shape)
        self._keys = ['name','nominal_resolution','grid','region']
        self._known = {
            (1920,2560): {'name':'N1280','nominal_resolution':'10km','grid':'N1280-A','region':'global'},
            (1921,2560): {'name':'N1280','nominal_resolution':'10km','grid':'N1280-B','region':'global'},
            (1205,1440): {'name':'O25','nominal_resolution':'25km','grid':'Orca025','region':'global'},
            (324,432) : {'name':'N216','nominal_resolution':'50km','grid':'N216-A','region':'global'},
            (325,432) : {'name':'N216','nominal_resolution':'50km','grid':'N216-B','region':'global'},
            (325,) : {'name':'N216','nominal_resolution':'50km','grid':'N216-ZM','region':'global'},
            (324,) : {'name':'N216','nominal_resolution':'50km','grid':'N216-ZM','region':'global'},
            # the following grid is use for unit testing
            (5,8) : {'name':'test','nominal_resolution':'huge','grid':'imaginary','region':'global'},
            'unknown': {'name':'unknown','nominal_resolution':'unknown','grid':'unknown','region':'unknown'}
        }
    def __getattr__(self, key):
        if key in self._keys:
            try:
                return self._known[self._shape][key]
            except KeyError:
                return self._known['unknown'][key]
        else:
            raise AttributeError(f"'{self.__class__.__name__}' has no information about '{key}'")

class LookupT:
    def __init__(self):
        self.inferred = {}
        self.bounds = {}

    def extract_cfstemporal(self, field, temporal_resolution=None):
        """ Extract the information needed for a CFS temporal domain.
        : field : a normal CF field
        : temporal_resolution : A tuple (interval, interval_units) (e.g 1,'mon')
                which should be used for the time domain. If not provided,
                it is inferred from the data
        """
        try:
            tdim = field.dimension_coordinate('T')
        except ValueError:
            # fixed data not valid at any time
            return {}
        
        ncvar = tdim.nc_get_variable(default=None)
        data = tdim.data
        if ncvar is None:
            bounds = float(data[0].array[0]),float(data[-1].array[0])
        else:
            if ncvar not in self.bounds:
                bounds = float(data[0].array[0]),float(data[-1].array[0])
                self.bounds[ncvar] = bounds
            else:
                bounds = self.bounds[ncvar]

        if temporal_resolution is None:
            if ncvar not in self.inferred:
                delta = (tdim[2].data-tdim[0].data)/2
                interval, interval_units = self.infer_temporal_resolution(tdim, delta)
                if ncvar is not None: 
                    self.inferred[ncvar] = interval, interval_units
            else:
                interval, interval_units = self.inferred[ncvar]
        else:
            interval, interval_units = temporal_resolution

        return {'interval':interval, 'interval_units':interval_units,'starting':bounds[0],
                'ending': bounds[1], 'units':tdim.units, 'calendar':tdim.calendar}
    
    def infer_temporal_resolution(self,tdim, delta):
        """
        Guess temporal resolution from cell spacing.
        : tdim : a cf time dimension coordinate construct 
        Will likely need fixing when we confront it with real data from the wild. 
        Thanks in advance David!
        """
        delta.Units = cf.Units('day')
        if delta < cf.TimeDuration(1,'day'):   #hours
            delta.Units = cf.Units('hour')
            return int(delta),'h'
        elif delta < cf.TimeDuration(28,'day'): 
            return int(delta),'d'
        elif delta < cf.TimeDuration(31,'day'):
            return 1,'m'
        elif delta> cf.TimeDuration(89,'day') and delta < cf.TimeDuration(93,'day'):
            return 3,'m'
        elif delta> cf.TimeDuration(359,'day') and delta < cf.TimeDuration(367,'day'): 
            return 1,'y'
        else:
            if tdim.calendar == '360_day':
                return int(delta/360),'y'
            else:
                return int(delta/360.25),'y'
            
    def infer_interval_from_coord(self, tdim):
        """ 
        For use where we are not looping over
        fields, directly infer time interval from a
        temporal dimension coordinate
        """
        delta = (tdim[2].data-tdim[0].data)/2
        return self.infer_temporal_resolution(tdim, delta)
        



def parse2atomic_name(field):
    """ 
    Make some sensible guesses for the "atomic_origin" of this 
    field.
    """
    names = ['project','mip','experiment','institution','source-id','variant-label','realm']
    values = [field.get_property(n, None) for n in names]
    use  = [v for v in values if v is not None]
    return '/'.join(use) 


def extract_cfsdomain(field, lookup_xy=LookupXY):
    """ 
    Given a CF field, extract the domain description understood
    by cfstore. E.g.
        {'name':'N216','region':'global','nominal_resolution':'10km',
            'size':1000,'coordinates':'longitude,latitude,pressure'}
    (Relies on a look up table for domain names and regions
    provided by teh user as a class which behaves like the Lookup
    class which is the default here.)
    : field : field from which we want the domain description
    : lookup_class : (Optional) A class which includes all the necessary domain descriptions for the
               file (or files) to be parsed. It should have the same signature as
               the default Lookup class provided in this module.
    : returns : A dictionary of domain properties for the field provided.
    """
    ydim = field.dimension_coordinate('Y').size
    try:
        xdim = field.dimension_coordinate('X').size
        shape = (ydim,xdim)
    except ValueError:
        shape = (ydim,)
    lookup = lookup_xy(shape)
    axis_names_sizes = field.domain._unique_domain_axis_identities()
    spatial_coords = ','.join(sorted([x for x in axis_names_sizes.values() if 'time' not in x]))
    domain_properties = {
        'size':field.size,
        'coordinates': spatial_coords,
        'nominal_resolution': lookup.nominal_resolution,
        'name':lookup.name,
        'region':lookup.region
    }
    return domain_properties


def parse_fields_todict(fields, temporal_resolution=None, lookup_xy=None, cfa=False):
    """
    Parse a list of cf-python fields into a list of properties suitable for loading into the database.
    : fields : a list of cf fields
    : temporal_resolution : either a known temporal resolution in the form n{h,d,m,y} or None.
            if none, we will infer temporal resolution by inspecting the time coordinate if 
            it exists.
    : lookup_class : optional. see description in cfparse_field_for_domain
    : cfa : True if aggregated fields
    : returns : a list of dictionaries of metadata properties describing
                each of the cf fields (a subset of the variables) found in the file.
    """
    descriptions = []
   
    # tools for handling domains and manifests
    cfahandler=CFAhandler(len(fields))
    if lookup_xy is None:
            lookup_xy=LookupXY
    lookup_t = LookupT()

     # loop over fields in file (not the same as netcdf variables)
    for v in fields:
        t1 = time()
        description = {'atomic_origin': parse2atomic_name(v), 'identity':v.identity()}
        if cfa:
            description['manikey'] = cfahandler.parse_field_to_manifest(v)
        properties = v.properties()
        for k in ['standard_name','long_name','realm']:
            description[k] = v.get_property(k,None)
            if description[k] is not None:
                properties.pop(k)
        t2 = time()
        description['time_domain'] = lookup_t.extract_cfstemporal(v, temporal_resolution)   
        t3 = time()
        description['spatial_domain'] = extract_cfsdomain(v, lookup_xy)
        t4 = time()
        cmlist = []
        for m, cm in v.cell_methods().items():
            for a in cm.get_axes():
                method = cm.get_method()
                cmlist.append((a,method))
        description['cell_methods'] = cmlist
        description['_proxied'] = {k:manage_types(v) for k,v in properties.items()}
        descriptions.append(description)
        t5 = time()
        logger.info(f'Parsing steps {t2-t1:.2f},{t3-t2:.2f},{t4-t3:.2f},{t5-t4:.2f} seconds')
    return descriptions, cfahandler.known_manifests



