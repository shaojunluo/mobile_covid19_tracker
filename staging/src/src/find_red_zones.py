import yaml
import os
import sys
# add the directory
sys.path.append(os.path.dirname(__file__) + '/..')
# read the relavant libaries
import lib.lib_tracking as utils
import lib.lib_model as model_utils

# read parameter for the step
with open(os.path.dirname(__file__) + '/../config_params.yaml','r') as f:
    params = yaml.safe_load(f)
    # elasticserch related params
    HOST_URL = params['elasticsearch']['records']['host']
    PORT = params['elasticsearch']['records']['port']
    HOST_MAP = params['elasticsearch']['maps']['host']
    PORT_MAP = params['elasticsearch']['maps']['port']
    n_thread = params['elasticsearch']['maps']['thread']
    # days to query
    d_before =   params['track.person']['day.before']
    d_after =    params['track.person']['day.after']
    lookup_day =   params['track.person']['lookup.day']
    R = params['track.contact']['distance.max']
    # I/O location
    patient_type = params['track.person']['person.type'] # read the patient of origin
    person_type = '1st_layer' # specify the person you track
    n_core = params['track.person']['num.cores']
    # now the input file is the risky contacts
    patient_file = params[patient_type]['folders']['deliver'] +f'/active_{patient_type}.csv'
    contact_file = params[patient_type]['folders']['deliver'] +f'/active_{person_type}.csv'
    patient_folder = params[patient_type]['folders']['track']
    contact_folder = params[patient_type]['folders']['track.contact']
    deliver_folder = params[patient_type]['folders']['deliver']
    prefix = params['ingestion']['prefix']

# get the clean list of persons
print(f'running query for "{person_type}"')
query_df = utils.processing_track_df(contact_file, person_type, 
                                     days_before = 0,  # we don't look up the earlier day
                                     days_after = d_before + d_after, # but we look the later day longer
                                     lookup_day= lookup_day,# if pivot day is none then take all time, default is today.
                                     prefix = prefix)

# querying elasticsearch and output to files (no need to parallel becasue it is fast)
active_contact = utils.track_persons(query_df, output_folder = contact_folder, host_url = HOST_URL, port = PORT,num_cores = n_core)
# save the new updated active first layer
utils.save_active_list(active_contact, deliver_folder, person_type)

print('Running for layer 0 red zones', end = ' ')
# get the final list of layer 0 redzone
df_patient = model_utils.detect_red_zones(patient_file, patient_type, patient_folder, R = R)
model_utils.deliver_red_zone(df_patient, file_name = deliver_folder + '/red_zones_layer_0.csv', 
                             index_name = prefix + 'red_zones_layer_0', host_url = HOST_MAP, port = PORT_MAP,n_thread=n_thread)
print('Running for layer 1 red zones', end = ' ')
# get the final list of layer 1 redzone
df_contact = model_utils.detect_red_zones(contact_file, person_type, contact_folder, R = R)
model_utils.deliver_red_zone(df_contact, file_name = deliver_folder + '/red_zones_layer_1.csv', 
                             index_name = prefix + 'red_zones_layer_1', host_url = HOST_MAP, port = PORT_MAP,n_thread=n_thread)

