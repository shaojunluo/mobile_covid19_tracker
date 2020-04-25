import yaml
import os
import sys

# add the directory
sys.path.append(os.path.dirname(__file__) + '/..')
# read the relavant libaries
import lib.lib_tracking as utils

# read parameter for the step
with open(os.path.dirname(__file__) + '/../config_params.yaml','r') as f:
    params = yaml.safe_load(f)
    # elasticserch related params
    HOST_URL = params['elasticsearch']['host']
    PORT = params['elasticsearch']['port']
    # days to query
    d_before =   params['track.person']['day.before']
    d_after =    params['track.person']['day.after']
    # I/O location
    person_type = params['track.person']['person.type'] # read the person you want to track
    in_file =    params[person_type]['input.file']
    out_folder = params[person_type]['folders']['track']
    deliver_folder = params[person_type]['folders']['deliver']

# get the clean list of personp
print(f'running query for "{person_type}"')
active_patients = utils.processing_track_df(in_file, person_type, 
                                         days_before = d_before, 
                                         days_after = d_after,
                                         pivot_day= None,# if pivot day is none then take all time, default is today.
                                         prefix = 'fortaleza_')

# querying elasticsearch and output to files (no need to parallel becasue it is fast)
active_patients = utils.track_persons(active_patients,output_folder = out_folder, 
                                      host_url = HOST_URL, port = PORT)
# save active patients for future use
utils.save_active_list(active_patients, deliver_folder, person_type)

