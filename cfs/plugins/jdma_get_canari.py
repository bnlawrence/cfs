#!/usr/bin/env python
# ------------------------------------------------------------------------------------------------
#
# Author: Rosalyn Hatcher       Date: January 2024
#
# Description:
#
# Usage: jdma_get.py [--filelist] [--start <year>] [--end <year>] [--ens <ensemble-num>] <suite-id>
#
# Options:
#    --filelist     - Write filepaths to file. Does not issue JDMA GET requests.
#                     List of files can then be passed to et_get.py
#
#    --start <year> - Year from which to start extracting data for
#
#    --end <year>   - Year upto and include to extract data for
#
#    --ens <num>    - Ensemble member number
#
# Example:
#    jdma_get.py --filelist --start 1950 --end 1952 --ens 5 u-cv247
#
# ------------------------------------------------------------------------------------------------

import os
import pwd
import sys

from jdma_client import jdma_lib
from jdma_client import jdma_common

# ==============================================================
# User Configurable Settings
# ==============================================================

# Print debug statements
DEBUG = False

# Number of months in a cycle
MONTHS_PER_CYCLE = 3

# If extracting files using JDMA set target location for extracted files. (ie. not using the --filelist option)
TARGET_DIR = '/work/xfc/vol9/user_cache/canari/downloads/SSP370'

# File types to extract
ATMOS_FILES = ['day_','mon_','1hr_','3hr_','6hr_pt_','day_z_','mon_z_','1hr_pt_','6hr_']
OCEAN_FILES = ['day__grid_T','mon__grid_T','mon__grid_U','mon__grid_V','mon__diaptr']
CICE_FILES = ['day','mon']

# ==============================================================
# End of User Configurable Settings
# ==============================================================

# Flags to indicate which models files are wanted from.
ATMOS = False
NEMO = False
CICE = False
if len(ATMOS_FILES) > 0:
    ATMOS = True
if len(OCEAN_FILES) > 0:
    NEMO = True
if len(CICE_FILES) > 0:
    CICE = True

class JDMAInterfaceError(Exception):
    pass


def get_user_login_name():
    "get a user login name"
    return pwd.getpwuid(os.getuid()).pw_name


class JDMAInterface():

    def __init__(self):
        username = get_user_login_name()
        self.username = username
        print('username: {}'.format(self.username))
        self.storage_type = 'elastictape'
        self.credentials = {}

    def submit_get(self, suite, ens_num, startdate, enddate, months, flist):
        """
        Submit a jdma GET job.
        """

        
        FILE_LIST = flist
        if FILE_LIST:
            # Output list of files to extract to a file
            filelist_out = '{}-{}-{}.filelist'.format(suite, startdate, enddate)
            print('Writing list of files to {}'.format(filelist_out))
            all_files = []
        else:
            # Extracting files using JDMA; set final destination for extracted files.
            #target_root = os.path.join('/work/xfc/vol9/user_cache/canari/downloads', suite)
            #target_root = os.path.join('/work/xfc/vol9/user_cache/canari/downloads/HIST1', suite)
            target_root = os.path.join(TARGET_DIR,suite)

            if not os.path.isdir(target_root): 
                try:
                    os.mkdir(target_root)
                except OSError as error:
                    print('Failed to make directory: {} \n {}'.format(target_root, error))
                    sys.exit(2)

        #for year in range(1950, 1951):
        for year in range(startdate, enddate+1):
            #for month in [1, 4, 7, 10]:
            for month in months:
                cycle = ''.join([str(year), str(month).zfill(2), '01T0000Z'])
                label = os.path.join(suite, cycle)
                if DEBUG:
                    print('label: {}'.format(label))

                if not FILE_LIST:
                    # Need a separate directory for each JDMA download request.
                    # JDMA sets permissions on the directory to root only. 
                    target_dir = os.path.join(target_root, cycle)
                    print('\nDownloading data to: {}'.format(target_dir))

                    if DEBUG:
                        print('Creating target directory: {}'.format(target_dir))

                    if not os.path.isdir(target_dir):
                        try:
                            os.mkdir(target_dir)
                        except OSError as error:
                            print('Failed to make directory: {} \n {}'.format(target_dir, error)) 
                            sys.exit(2)
            
                # Get batch id for this cycle
                batch_id = self._get_batch_id_for_path(label)
                if DEBUG:
                    print('batch id for this cycle: {}'.format(batch_id))

                if batch_id == None:
                    raise JDMAInterfaceError(('Failed to find batch id with label: {}'
                                              ).format(label))

                # Get path of original data
                path = self._get_batch_orig_path(batch_id, label)

                # Get list of files to pull from ET
                filelist = self._get_filelist(year, month, suite, ens_num)
                filelist = [os.path.join(path, f) for f in filelist]

                if DEBUG:
                    print('filelist: {}'.format(filelist))

                if FILE_LIST:
                    all_files = all_files + filelist
                else:
                    # Issue jdma download request
                    resp = jdma_lib.download_files(
                        self.username,
                        batch_id=batch_id,
                        filelist=filelist,
                        target_dir=target_dir,
                        credentials=self.credentials)

                    if DEBUG:
                        print('Status Code: {}'.format(resp.status_code))

                    req_id = self._resp_to_req_id(resp)
                    print('Request Id: {}'.format(req_id))
        
        if FILE_LIST:
            with open(filelist_out, 'w') as outfile:
                for line in all_files:
                    outfile.write(line+'\n')            
            req_id = 0

        return req_id
        

    def _get_filelist(self, year, month, suite, ens_num):

        m = month
        y = year
        runid = suite.split('-')[1] 


        prefixes = []
        filelist = []

        # Atmos Files
        if ATMOS:
            suite_prefix = ''.join([runid,'a'])
            if ens_num:
                suite_prefix = '_'.join([suite_prefix,str(ens_num)])

            orig_times = ['day_','mon_','1hr_','3hr_','6hr_pt_']
            times = ['day_','mon_','1hr_','3hr_','6hr_pt_','day_z_','mon_z_','1hr_pt_','6hr_']
            times = ATMOS_FILES

            for time in times:
                prefix = '_'.join([suite_prefix, time])
                prefixes.append(prefix)

        # Ocean Files
        if NEMO:
            suite_prefix = ''.join([runid,'o'])
            if ens_num:
                suite_prefix = '_'.join([suite_prefix,str(ens_num)])

            #ocean_times = ['day__grid_T','mon__grid_T','mon__grid_U','mon__grid_V','mon__diaptr']
            ocean_times = OCEAN_FILES

            for time in ocean_times:
                prefix = '_'.join([suite_prefix, time])
                prefixes.append(prefix)

        if ATMOS or NEMO:
            if DEBUG:
                print('prefixes: {}'.format(prefixes))

            for x in range(MONTHS_PER_CYCLE):
                date = ''.join([str(y),str(m).zfill(2)])
                suffix = ''.join([date,'-',date,'.nc'])
                if DEBUG:
                    print('suffix: {}'.format(suffix))
                m = m + 1

                for p in prefixes:
                    filelist.append('_'.join([p, suffix]))

        # Ice Files
        if CICE:
            suite_prefix = ''.join([runid,'i'])
            if ens_num:
                suite_prefix = '_'.join([suite_prefix,str(ens_num)])

            #ice_times = ['day','mon']
            ice_times = CICE_FILES

            prefixes = []
            for time in ice_times:
               prefix = '_'.join([suite_prefix, time])
               prefixes.append(prefix)
        
            m = month
            for x in range(MONTHS_PER_CYCLE):
                date1 = ''.join([str(y),str(m).zfill(2),'01'])
                if m == 12:
                    m = 0
                    y = y + 1
                date2 = ''.join([str(y),str(m+1).zfill(2),'01'])
                suffix = ''.join([date1,'-',date2,'.nc'])
                if DEBUG:
                    print('suffix: {}'.format(suffix))
                m = m + 1

                for p in prefixes:
                    filelist.append('_'.join([p, suffix]))

        print('filenames: {}'.format(filelist))

        return filelist     
        

    def _get_batch_orig_path(self, batch_id, label):

        resp = jdma_lib.get_files(self.username, batch_id=batch_id)

        try:
            fields = resp.json()
        except ValueError:
            raise JDMAInterfaceError('unparseable response from JDMA')

        status_code = resp.status_code
        if status_code != 200:
            if 'error' in fields:
                raise JDMAInterfaceError('JDMA request failed with HTTP status code {} and message: {}'
                                         .format(status_code, fields['error']))
            else:
                raise JDMAInterfaceError('JDMA request failed with HTTP status code {}'
                                         .format(status_code))

        file = fields['migrations'][0]['archives'][0]['files'][0]['path']
        orig_path = os.path.dirname(file)

        if DEBUG:
            print('Original batch file path: {}'.format(orig_path))

        return orig_path


    def _resp_to_req_id(self, resp):
        """
        returns the request ID in the response from JDMA, 
        or if status code was not 200, raises an exception with 
        the error.
        """
        try:
            fields = resp.json()
        except ValueError:
            raise JDMAInterfaceError('unparseable response from JDMA')

        status_code = resp.status_code

        if status_code == 200:
            try:
                return fields['request_id']
            except KeyError:
                raise JDMAInterfaceError('no request ID in JDMA response')
            
        elif 'error' in fields:
            raise JDMAInterfaceError('JDMA request failed with HTTP status code {} and message: {}'
                                     .format(status_code, fields['error']))
        else:
            raise JDMAInterfaceError('JDMA request failed with HTTP status code {}'
                                     .format(status_code))


    def _get_batch_id_for_path(self, path):
        id = self._get_batch_id_for_path2(path)
        must_exist = 1
        if id == None and must_exist:
            raise JDMAInterfaceError('could not find batch on storage for path {}'.format(path))
        else:
            return id


    def _get_batch_id_for_path2(self, path):
        """
        Look up the batch with label = the supplied path
        and whose location is 'ON_STORAGE'
        """

        workspace = 'canari'

        resp = jdma_lib.get_batch(self.username,
                                  workspace=workspace,
                                  label=path)

        if resp.status_code != 200:
            if resp.status_code % 100 == 5:
                sys.stderr.write(('Warning: JDMA responded with status code {} when checking for '
                                  'existing batches. Assuming none found.\n'
                                  ).format(resp.status_code))
            return None

        resp_dict = resp.json()

        if 'migrations' in resp_dict:
            batches = resp_dict['migrations']
        else:
            batches = [resp_dict]
        
        batch_ids = [batch['migration_id'] for batch in batches 
                     if jdma_common.get_batch_stage(batch['stage']) == 'ON_STORAGE']
    
        num_matches = len(batch_ids)

        if num_matches == 0:
            return None

        elif num_matches == 1:
            return batch_ids[0]

        else:
            raise JDMAInterfaceError('found more than one batch on storage for path {} (ids={})'
                                     .format(path,
                                             ','.join(map(str, batch_ids))))
        

if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=int, help="Year from which to start extracting data from")
    parser.add_argument('--end', type=int, help="Year to extract data up to")
    parser.add_argument('--cycle', type=int, help="Cycle (YYYYMM) for which to extract data for. Cannot be used in combination with --start/--end")
    parser.add_argument('--filelist', action="store_true", help="Create file(s) containing a list of files that would be extracted")
    parser.add_argument('suite', help="Suite ID to extract data from")
    parser.add_argument('--ens', type=int, help="Ensemble number of suite")
    args = parser.parse_args()

    if (args.start or args.end) and args.cycle:
        print('Invalid Options: --start/--end cannot be used with --cycle')
        parser.print_help()
        sys.exit(1)

    #suite = 'u-cv625'
    #ens_num = 2
    suite = args.suite
    #ens_num = args.ens_num
    if args.ens:
        ens_num = args.ens
    else:
        ens_num = None

    if args.start:
        start = args.start
    else:
        start = 1950

    if args.end:
        end = args.end
    else:
        end = 2014
    
    period = 'Jan {} to Dec {}'.format(start, end)

    if args.cycle:
        cycle = str(args.cycle)
        start = int(cycle[:4])
        end = start
        months = [int(cycle[-2:])]
        period = 'cycle {}'.format(cycle)
    else:
        months = []
        for i in range(1,13,MONTHS_PER_CYCLE):
            months.append(i)

    if args.filelist:
        print('Filelist true')
        flist = True
        action = 'Writing list of files'
    else:
        flist = False
        action = 'Extracting files'

    print('{} for suite: {} (Ens Member {}), for {}'.format(action, suite, ens_num, period))

    inst = JDMAInterface()
    inst.submit_get(suite, ens_num, start, end, months, flist)




