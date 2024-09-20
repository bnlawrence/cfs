from pathlib import Path
import h5netcdf as h5
import numpy as np
from time import time
import logging
logger = logging.getLogger(__name__)

class CFAhandler:
    def __init__(self, expected_files):
        """ Opens the aggregation file for further parsing.
        We need to cover the situation where the variables of the 
        aggregation use different fragment sets with differing bounds.
        We want to avoid file and variable handling if we can.
        """
        self.dataset = None
        self.known_files = {}
        self.expected = expected_files
        self.parsed = 0
        self.known_coordinates = {}

    def parse_fragment_info(self,field, accessor=None):
        """
        Should return the necessary information for a fragment to be located in coordinate 
        space. For now I just return some best guesses. I need David to help me find out
        how to get at the cfa_locations variable ...   
        : field : a cfa field
        : accessor : if provided, can be used to get additional information from the 
                     storage about the file (e.g. format, checksum, size etc)  
        """
        #FIXME: All this file handling is brittle. Help David!
        t1 = time()
        self.parsed+=1
        files =  field.get_filenames()
        if self.dataset is None:
            cfa_file = [f for f in files if Path(f).suffix == '.cfa'][0]
            self.dataset = h5.File(cfa_file,'r')
        
        #can't assume all fields in a cfa file have the same filenames
        filenames = sorted([f for f in files if Path(f).suffix !='.cfa'])
        filedetails = []
        for f in filenames:
            if f not in self.known_files:
                if accessor is not None:
                    self.known_files[f] = accessor(f)
                else:
                    self.known_files[f] = {'path':f,'type':'F','name':Path(f).name}
            filedetails.append(self.known_files[f])
        
        ncvar = field.nc_get_variable()
        alocations = self.__get_cfalocation(ncvar)
        tdim = field.dimension_coordinate('T', default=None)
       
        if tdim is None:
            newbounds, units, calendar = None, None, None
        else: 
            tdimvar = tdim.nc_get_variable()
            if tdimvar not in self.known_coordinates:
                if tdim.has_bounds():
                    bounds = tdim.bounds.data.array
                    #FIXME: Do this with numpy
                    newbounds = []
                    left=0
                    for n in alocations:
                        right = left + n -1
                        newbounds.append([bounds[left][0],bounds[right][1]])
                        left+=n
                    self.known_coordinates[tdimvar] = np.array(newbounds)
                else:
                    raise NotImplementedError
            newbounds = self.known_coordinates[tdimvar]  
            units, calendar = tdim.units, tdim.calendar
            
        t2 = time()-t1
        logger.info(
            f"Found fragment info for '{field.identity()}' in {t2:.2f}s ({self.parsed}/{self.expected})")

        keys = ['fragments','bounds','cells','units','calendar']
        return {k:v for k,v in zip(keys,[filedetails, newbounds, alocations, units, calendar])}
    
    def __get_cfalocation(self,ncvar):
        """ Find the CFA location by going down the rabbit hole"""
        ncv = self.dataset.variables[ncvar]
        aggregated_data = ncv.attrs['aggregated_data']
        parsed_aggregated_data = dict(zip(aggregated_data.split()[::2], aggregated_data.split()[1::2]))
        location = parsed_aggregated_data['location:']
        #FIXME: this assumes time is the first dimension 
        location = self.dataset.variables[location][:][0]
        return location