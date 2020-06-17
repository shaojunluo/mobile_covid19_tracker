import yaml
from glob import glob
import os
import sys

sys.path.append(os.path.dirname(__file__) + '/..')
import lib.lib_model as utils

# read parameter and dependencies of the step
with open(os.path.dirname(__file__) + '/../config_params.yaml','r') as f:
    params = yaml.safe_load(f)
    # choose the type of person we want to run
    person_type = params['track.person']['person.type']
    # I/O settings
    deliver_folder =  params[person_type]['folders']['deliver']
    input_folder = params[person_type]['folders']['contact']
    num_cores = params['track.contact']['num.cores']
    # get input files
    input_file = deliver_folder + f'/active_{person_type}.csv'

# read the files according to the files (here we use all patients)
query_files = utils.retrieve_active_patients(input_file, person_type, input_folder)

print(len(query_files))
# get the subset of close contact patient
df = utils.select_close_contact_subset(input_file, person_type, query_files, n_workers = num_cores)
# save patient contact files
df.to_csv(deliver_folder + '/contact_patient_to_patient.csv',index = False)

# count contacts
hit_network = df.groupby(['sourceId','targetId']).size().reset_index().rename(columns ={0:'hit'})
hit_network.to_csv(deliver_folder  +'/contact_patient_to_patient_summary.csv', index = False)
