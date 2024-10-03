import cf
import re
from h5netcdf.legacyapi import Dataset

import logging
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def get_tm(field):
    """ For the given field, get the time cell method, if any, as a string """
    # FIXME strictly, the name of a dimension might not include time, so
    # we ought to look at any axes to see if it has standard name of time,
    tm = ''
    for m, cm in field.cell_methods().items():
        for a in cm.get_axes():
            if 'time' in a:
                tm += str(cm)
    return tm

def fix_canari_coordinates(field):
    """ 
    This changes the actual time coordinate and time bounds
    data by adding the interval offset to the values which
    are present. It should only be used when the check
    carried out in check_for_canari_metadata_issues 
    determines that it is necessary.
    """
    interval_offset = field.get_property('interval_offset',0)
    if interval_offset in [0, '0ts','h']:
        logging.info('No coordinate fix needed for {field}')
    else:
    
        time_data = field.coordinate('time')
        time_data = time_data + interval_offset
        logging.info('Fixed time data for {field}')
        
        if time_data.has_bounds():
            bounds = time_data.bounds.data.array
            bounds[:,0] = bounds[:,0] + interval_offset
            bounds[:,1] = bounds[:,1] + interval_offset - cf.Data(1.0,'d')
            logging.info('Fixed time  bounds for {field}')

def check_for_canari_metadata_issues(field, filename, extra_properties=[], fix_meta=True, fix_coords=False):
    """ 
    All CANARI FILES were produced without correct frequency metadata.
    This routine can fix all these fields if fix=True.
    
    Some CANARI files were produced with incorrect metadata.
    This routine parses a CF field to see if it is one of those,
    and if fix=True, it will fix it.
    In those cases, the coordinate and bounds metdata may be incorrect
    as well, if fix_coords is true, that too is fixed.
    """

    try:
        frequency, iwrite, iop, tm, filename = get_frequency(field,handler=canari_v1ahandler)
    except ValueError as err:
        # first error, hourly mean data written to the 1hr point file
        flux_in_wrong_file = 'm01s01i235'
        if field.nc_get_variable() == flux_in_wrong_file:
            # this field was erroneously put in the point file, but it is mean data.
            frequency = '1hr'
            logger.info(f'Noted {field} ({flux_in_wrong_file} was in the wrong file')
            iop = field.get_property('interval_operation','')
            iwrite = field.get_property('interval_write','')
        else:
            # second error, some of these were wrong in some simulations ... 
            if get_tm(field) == '' and field.get_property('online_operation','') == 'average':
                if fix_meta:
                    iop = field.get_property('interval_operation','')
                    cm = cf.CellMethod(axes='time',method='mean')
                    cm.intervals=[cf.Data(iop,'seconds')]
                    field.set_construct(cm)
                    logger.info(f'Fixed missing cell method {cm} for {field.identity()}')
                logger.warning(str(err))
                iwrite = field.get_property('interval_write','')
                frequency=_write2freq(iwrite, str(cm), field)
            else:
                if field.identity() == 'surface_altitude':
                    frequency='fx'
                else:
                    frequency='unknown'
                    logger.warning(str(err))
    if fix_meta:
        # third error, a number of issues with file metadata, documented here: https://github.com/NCAS-CMS/canari/issues/4
        field.set_property('frequency',frequency)
        field.set_property('variant_label',field.get_property('variant_id'))
        field.del_property('variant_id')
        field.set_property('source_id', field.get_property('source_index'))
        field.del_property('source_index')
        field.set_property('parent_activity_id', field.get_property('parent_source_id'))
        field.set_property('parent_source_id', field.get_property('source_id'))
        fbits = filename.split('_')
        field.set_property('runid',fbits[2])
        logger.info(f'Fixed inherited file attributes for {field.identity()} ({frequency})')
    
    else:
         logger.warning(f'Did not fix inherited file attributes for {field}.')

    if frequency in ['fx','uknown']:
        return
    
    # fourth error, for some data, offsets were not properly used.
    # Only looking for fields which are a 30 day mean sampled every 24 hours
    interval_operation = iop
    online_operation = field.get_property('online_operation','')
    if online_operation not in ['mean','average'] or interval_operation != '24 h' or iwrite != '1 month':
        return
    
    cell_methods = field.cell_methods()
    deletable = []
    for k,v in cell_methods.items():
        if str(v).startswith('time'):
            deletable.append(k)
    if len(deletable) > 1:
        raise ValueError(f"Expecting only one time method, got more: {deletable}")
    k=deletable[0]
    if fix_meta:
        new_1 = cf.CellMethod(axes='time', method='point', qualifiers={'within': 'days'})
        new_2 = cf.CellMethod(axes='time', method='mean', qualifiers={'over': 'days'})
        field.del_construct(k)
        field.set_construct(new_1)
        field.set_construct(new_2)
        logger.info(f'Replaced time cell method for {field.identity()}')
    else:
        logger.warning(f'Did not fix cell methods for {field.identity()}')

    if fix_coords:
        fix_canari_coordinates()


def _write2freq(string, method, field):
    """
    Given an XIOS interval write string and 
    a cf cell method, convert it to look like a CMIP6 string.
    :param string: an XIOS interval_write string
    :param method: a CF python cell method string
    :return: A cmip frequency string
    """ 
    try:      
        num,units = string.split(' ')
    except:
        logger.warning(f'Cannot parse string into number and units for string {string}, method{method} and field {field.identity()}')
        return string
    ptdata = method == 'time: point' or method==''
    convert = {'month':'mon','d':'day','h':'hr'}
    if num == '1' and not units == 'h':
        num =''
    result = f'{num}{convert[units]}'
    if ptdata:
        result+='_pt'
    return result


def get_frequency(field, handler=None):
    """
    Gets all the variants of frequency information which we know
    about from the file and, if the handler is provided, the 
    filename. The handler should be a function which can extract
    the frequency information from the filename. Typically different 
    handlers will be necessary for different data origins. 
    """


    frequency = field.get_property('frequency','')
    iwrite = field.get_property('interval_write','')
    iop = field.get_property('interval_operation','')
    usefile =None
    tm = get_tm(field)

    if handler is not None:
        
        filenames = list(field.get_filenames())
        try:
            usefile = filenames.pop()
            while not usefile.endswith('nc'):
                usefile = filenames.pop()
            frequency, filename = handler(usefile)
        except IndexError:
            raise ValueError('No valid nc filename found for field')
        
        if _write2freq(iwrite, tm, field) != frequency:
            if bool(re.search(r'_(\d+)[^a-zA-Z]*', frequency)):
                #it's ok, cmip6 doesn't do this (stick a number between
                #in after the mean value)
                pass
            else:
                oop = field.get_property('online_operation','')            
                print(f'Problem for {field.identity()} in {filename}')
                print(f'interval_write {iwrite}; interval_operation {iop}; cell method {tm} ; online operation {oop}.')
                print(f'Calculation was [{_write2freq(iwrite,tm, field)}] compared with {frequency}]')
                raise ValueError(f'Inconsistent filename and internal frequency {field.identity()} in {filename}')

    return frequency,iwrite, iop, tm, filename

def canari_v1ahandler(fn):
    """ 
    Given a canari output filename, return frequency string.
    Expect input filename of the form:
    'unavailable:cs125_1_6hr_u_pt_cordex__195104-195104.nc'
    or
    'cs125_1_6hr_u_pt_cordex__195104-195104.nc'
    This version is the one for the original data as written
    to JASMIN from ARCHER2.
    """
    if not fn.endswith('.nc'):
        raise ValueError(f'Canari handler needs a netcdf file, got {fn}')
    bits = fn.split(':')
    filebit = bits[len(bits)-1]
    #clean the grid information out 
    cleaned = re.sub(r'(_[uvtz])', '', filebit).replace('cordex','')
    parts = cleaned.split('_')
    fbit = '_'.join(parts[2:4]).rstrip('_')

    return fbit,filebit
   

class LookupT:
    """
    Provides tools for efficiently extracting time domain information from within a file.
    The efficiency arises from constructing a lookup table of time information based on
    time coordinate variable reuse within the file.
    """
    def __init__(self):
        """
        Initialisation provides the dictionaries used for holding time coordinate
        information between variables.
        """        
        self.inferred = {}
        self.bounds = {}
        self.handler= None

    def extract_cfstemporal(self, field):
        """
        Extracts the information needed for a cfs temporoal domain
        as a dictionary. 
        
        :param field: input field
        :type field: a cf field construct
        :return:  A dictionary defining the time domain associated with
                  the field.
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

        if ncvar not in self.inferred:
            delta = (tdim[2].data-tdim[0].data)/2
            
            try:
                interval, interval_offset, interval_units = self.xios_resolution(field)
            except ValueError as err:
                if 'XIOS' in str(err):
                    interval_offset=None
                    interval, interval_units = self.infer_temporal_resolution(tdim, delta)
                else:
                    raise
            if ncvar is not None: 
                self.inferred[ncvar] = interval, interval_units,interval_offset
        else:
            interval, interval_units, interval_offset = self.inferred[ncvar]

        return {'interval':interval, 'interval_units':interval_units,'interval_offset':interval_offset,
                'starting':bounds[0], 'ending': bounds[1], 'units':tdim.units, 'calendar':tdim.calendar}
    
    def infer_temporal_resolution(self,tdim, delta):
        """
        Guess temporal resolution, based on cell methods and 

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

    def xios_resolution(self, field):
        """ 
        Use the XIOS information written into the field
        """
        try:
             interval = field.get_property('interval_write')
        except:
            raise ValueError('No XIOS interval information')
        offset = field.get_property('interval_offset',"0 h")
        m = re.match(r'(\d+(?:\.\d*)?|\.\d+) *(\w+)',interval)
        value= int(m.group(1))
        units= m.group(2)
        m = re.match(r'(\d+(?:\.\d*)?|\.\d+) *(\w+)',offset)
        offset=int(m.group(1))
        return value, offset, units
        

        