from cfs.db.time_handling import LookupT
from cfs.db.project_config import ProjectInfo
import numpy as np
from cfs.db.cfa_tools import CFAhandler
from time import time
import logging
import re
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

def parse2atomic_name(field, atomic_params):
    """ 
    Make some sensible guesses for the "atomic_origin" of this 
    field.
    """
    values = [field.get_property(n, None) for n in atomic_params]
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
    # I don't think we'd have data files without Y would we?
    try:
        ydim = field.dimension_coordinate('Y').size
    except ValueError:
        raise NotImplementedError("We didn't anticiapte data without a Y coordinate!")
    try:
        xdim = field.dimension_coordinate('X').size
        shape = (ydim,xdim)
    except ValueError:
        shape = (ydim,)
    lookup = lookup_xy(shape)
    axis_names_sizes = field.domain._unique_domain_axis_identities()
    spatial_coords = [x for x in axis_names_sizes.values() if 'time' not in x]
    size = np.prod([int(re.search(r'\d+', x).group()) for x in spatial_coords])
    spatial_coords = ', '.join(sorted(spatial_coords))
    domain_properties = {
        'size':size,
        'coordinates': spatial_coords,
        'nominal_resolution': lookup.nominal_resolution,
        'name':lookup.name,
        'region':lookup.region
    }
    return domain_properties


def parse_fields_todict(fields, lookup_xy=None, vocab=None, cfa=False):
    """
    Parse a list of cf-python fields into a list of properties suitable for loading into the database.
    : fields : a list of cf fields
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

    if vocab is not None:
        info = ProjectInfo()
        atomic_params = info.get_atomic_params(vocab)
        default_params = info.get_facets(vocab)
        for f in ['standard_name','long_name','realm','frequency']:
            if f not in default_params:
                default_params.append(f)
    else:
        atomic_params = ['project','mip','experiment','institution','source-id','variant-label','realm']
        default_params = ['standard_name','long_name','realm','source_id','frequency',
                  'source','variant_label','experiment','runid']
        
     # loop over fields in file (not the same as netcdf variables)
    for v in fields:
        t1 = time()
        description = {'atomic_origin': parse2atomic_name(v, atomic_params), 'identity':v.identity()}
        if cfa:
            description['manikey'] = cfahandler.parse_field_to_manifest(v)
        properties = v.properties()
        for k in default_params:
            description[k] = v.get_property(k,None)
            if description[k] is not None:
                properties.pop(k)
            else:
                description.pop(k)
        t2 = time()
        description['time_domain'] = lookup_t.extract_cfstemporal(v)   
        t3 = time()
        description['spatial_domain'] = extract_cfsdomain(v, lookup_xy)
        t4 = time()
        cmlist = []
        for m, cm in v.cell_methods().items():
            for a in cm.get_axes():
                if hasattr(cm,'intervals'):
                    intervals = cm.intervals
                else:
                    intervals=None
                cmlist.append((a,cm.get_method(),cm.qualifiers(),intervals))
        description['cell_methods'] = cmlist
        description['_proxied'] = {k:manage_types(v) for k,v in properties.items()}
        descriptions.append(description)
        t5 = time()
        logger.info(f'Parsing steps {t2-t1:.2f},{t3-t2:.2f},{t4-t3:.2f},{t5-t4:.2f} seconds')
    return descriptions, cfahandler.known_manifests



