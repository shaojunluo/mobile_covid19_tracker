import yaml
import os
import sys
from functools import partial
from multiprocessing import Pool

# add the directory
sys.path.append(os.path.dirname(__file__) + '/..')
# read the relavant libaries
import lib.lib_close_contact as utils
import lib.lib_model as model_utils

# read parameter of the step
with open(os.path.dirname(__file__) + '/../config_params.yaml', 'r') as stream:
    params = yaml.safe_load(stream)
    # choose the type of person we want to run
    person_type = params['track.person']['person.type']
    # elasticsearch related parameters
    HOST_URL = params['elasticsearch']['records']['host']
    PORT = params['elasticsearch']['records']['port']

    # I/O settings
    input_folder = params[person_type]['folders']['track']
    output_folder = params[person_type]['folders']['contact']
    result_folder = params[person_type]['folders']['contact.profile']
    deliver_folder = params[person_type]['folders']['deliver']
    # query parameters
    filter_rule = params['track.contact']['filter.rule']
    m_before = params['track.contact']['minute.before']
    m_after =  params['track.contact']['minute.after']
    max_d =    params['track.contact']['distance.max']
    num_cores =params['track.contact']['num.cores']
    prefix = params['ingestion']['prefix']

print(f'running query for "{person_type}"')
# read files
files = model_utils.retrieve_active_patients(deliver_folder + f'/active_{person_type}.csv', person_type, input_folder)
# Track the list of close contact
func = partial(utils.track_close_contact, output_folder = output_folder, person_type = person_type, filtering = filter_rule, 
                                          minutes_before = m_before, minutes_after = m_after, distance = max_d,
                                          host_url = HOST_URL, port = PORT, index_prefix = prefix)

# Parrellel processing patient to all 
with Pool(num_cores) as p:
    dfs = list(p.map(func, files))

# get the summary
utils.close_contact_summary(dfs, deliver_folder)
# update the new generated files
files = model_utils.retrieve_active_patients(deliver_folder + f'/active_{person_type}.csv', person_type, output_folder)
# shorten the list
utils.shorten_close_contact(files, result_folder)

