
import cf
import h5netcdf as h5
import numpy as np
from core.db.cfa_tools import CFAhandler

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

class Lookup:
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

def parse2atomic_name(field):
    """ 
    Make some sensible guesses for the "atomic_origin" of this 
    field.
    """
    names = ['project','mip','experiment','institution','source-id','variant-label','realm']
    values = [field.get_property(n, None) for n in names]
    use  = [v for v in values if v is not None]
    return '/'.join(use) 


def extract_cfsdomain(field, lookup_class=Lookup):
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
    shape = field.construct('Y').size, field.construct('X').size
    lookup = lookup_class(shape)
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

def extract_cfstemporal(field, temporal_resolution=None):
    """ Extract the information needed for a CFS temporal domain.
    : field : a normal CF field
    : temporal_resolution : A tuple (interval, interval_units) (e.g 1,'mon')
            which should be used for the time domain. If not provided,
            it is inferred from the data
    """    
    tdim = field.construct('T')
    if temporal_resolution is None:
        interval, interval_units = infer_temporal_resolution(tdim)
    else:
        interval, interval_units = temporal_resolution
    data = field.construct('T').data
    return {'interval':interval, 'interval_units':interval_units,'starting':float(data[0].array[0]),
            'ending': float(data[-1].array[0]), 'units':tdim.units, 'calendar':tdim.calendar}


def infer_temporal_resolution(tdim):
    """
    Guess temporal resolution from cell spacing.
    : tdim : a cf time dimension coordinate construct 
    Will likely need fixing when we confront it with real data from the wild. 
    Thanks in advance David!
    """
    delta = (tdim[2].data-tdim[0].data)/2
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


def parse_fields_todict(fields, temporal_resolution=None, lookup_class=None, cfa=False):
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
    # loop over fields in file (not the same as netcdf variables)
    if cfa:
        cfahandler=CFAhandler()
    for v in fields:
        description = {'atomic_origin': parse2atomic_name(v), 'identity':v.identity()}
        if cfa:
            description['cfa'] = cfahandler.parse_fragment_info(v)
        properties = v.properties()
        for k in ['standard_name','long_name','realm']:
            description[k] = v.get_property(k,None)
            if description[k] is not None:
                properties.pop(k)
        description['time_domain'] = extract_cfstemporal(v, temporal_resolution)

        if lookup_class is None:
            lookup_class=Lookup
        description['spatial_domain'] = extract_cfsdomain(v, lookup_class)

        cmlist = []
        for m, cm in v.cell_methods().items():
            for a in cm.get_axes():
                method = cm.get_method()
                cmlist.append((a,method))
        description['cell_methods'] = cmlist
        description['_proxied'] = {k:manage_types(v) for k,v in properties.items()}
        descriptions.append(description)
    return descriptions



