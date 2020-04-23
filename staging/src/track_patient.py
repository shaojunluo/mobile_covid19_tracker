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

# get the clean list of personp
print(f'running query for "{person_type}"')
query_df = utils.processing_track_df(in_file, person_type, 
                                         days_before = d_before, 
                                         days_after = d_after,
                                         prefix = 'fortaleza_')

# querying elasticsearch and output to files (no need to parallel becasue it is fast)
utils.track_persons(query_df,output_folder = out_folder, 
                        host_url = HOST_URL, port = PORT)
