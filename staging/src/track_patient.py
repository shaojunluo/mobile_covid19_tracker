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
    HOST_URL = params['elasticsearch']['records']['host']
    PORT = params['elasticsearch']['records']['port']
    # days to query
    d_before =   params['track.person']['day.before']
    d_after =    params['track.person']['day.after']
    lookup_day =   params['track.person']['lookup.day'] # latest date
    # I/O location
    n_core  = params['track.person']['num.cores']
    person_type = params['track.person']['person.type'] # read the person you want to track
    in_file =    params[person_type]['input.file']
    out_folder = params[person_type]['folders']['track']
    deliver_folder = params[person_type]['folders']['deliver']
    prefix = params['ingestion']['prefix']

# get the clean list of personp
print(f'running query for "{person_type}"')
active_patients = utils.processing_track_df(in_file, person_type, 
                                         days_before = d_before, 
                                         days_after = d_after,
                                         lookup_day= lookup_day,
                                         prefix = prefix)


# querying elasticsearch and output to files (no need to parallel becasue it is fast)
active_patients = utils.track_persons(active_patients,output_folder = out_folder, 
                                      host_url = HOST_URL, port = PORT, num_cores = n_core)
# save active patients for future use
utils.save_active_list(active_patients, deliver_folder, person_type)

