import yaml
import lib_utils

# specify the person you want to track
person_type = 'patient.higor'

# read parameter for the step
with open('config_params.yaml', 'r') as stream:
    params = yaml.safe_load(stream)
    HOST_URL = params['elasticsearch']['host']
    PORT = params['elasticsearch']['port']
    in_file =    params['track.person'][person_type]['input']
    out_folder = params['track.person'][person_type]['output.folder'] 
    d_before =   params['track.person'][person_type]['day.before']
    d_after =    params['track.person'][person_type]['day.after']

# get the clean list of person
query_df = lib_utils.processing_track_df(in_file, person_type, 
                                         days_before = d_before, 
                                         days_after = d_after,
                                         prefix = 'fortaleza_')

# querying elasticsearch and output to files (no need to parallel becasue it is fast)
lib_utils.track_persons(query_df,output_folder = out_folder, 
                        host_url = HOST_URL, port = PORT)
