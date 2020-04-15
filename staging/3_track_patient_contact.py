import yaml
from glob import glob
import lib_utils

# read parameter and dependencies of the step
with open('config_params.yaml', 'r') as stream:
    params = yaml.safe_load(stream)
    # choose the type of person we want to run
    person_type = params['track.person']['person.type']
    # I/O settings
    input_file =  params[person_type]['input.file']
    input_folder = params[person_type]['folders']['close_contact']
    result_folder = params[person_type]['folders']['result']


query_files = glob(input_folder + '/*.csv') # read the files 
# get the subset of close contact patient
df = lib_utils.select_close_contact_subset(input_file, person_type,query_files, n_workers = 12)
# df.save 
df.to_csv(result_folder + '/close_contact_patient.csv',index = False)

# count hits
hit_network = df.groupby(['sourceId','targetId']).size().reset_index().rename(columns ={0:'hit'})

hit_network.to_csv(result_folder  +'/close_contact_patient_summary.csv', index = False)
    
