"""
Created on 2020-4-23 
@author: Matteo Serafino, Shaojun Luo
"""
import os
import sys
import yaml
from datetime import datetime
from multiprocessing import Pool
from functools import partial

# add the directory
sys.path.append(os.path.dirname(__file__) + '/..')
# read the relavant libaries
import lib.lib_model as utils

# read parameter and dependencies of the step
with open(os.path.dirname(__file__) + '/../config_params.yaml','r') as f:
    params = yaml.safe_load(f)
    # host for elasticsea
    HOST_URL = params['elasticsearch']['users']['host']
    PORT = params['elasticsearch']['users']['port']
    n_thread = params['elasticsearch']['users']['thread']
    # choose the type of person we want to run
    person_type = params['track.person']['person.type']
    # I/O setting
    input_folder = params[person_type]['folders']['contact.profile']
    output_folder = params[person_type]['folders']['deliver']
    num_cores = params['track.contact']['num.cores']
    # query parameters
    rho     =  params['contact.model']['rho']
    m_after =  params['track.contact']['minute.after']
    max_d =    params['track.contact']['distance.max']
    num_cores =params['track.contact']['num.cores']
    prefix = params['ingestion']['prefix']


# get the list of files for execute
files = utils.retrieve_active_patients(output_folder +f'/active_{person_type}.csv', person_type, input_folder)

# assign probability
with Pool(num_cores) as p:
    func = partial(utils.probabilistic_model, _time = m_after, R = max_d,model='continuos')
    result = list(p.map(func, files))
# cancat result
risky_contact = utils.concat_files(result)
# final result delivery
risky_contact = utils.calculate_risky_contact(risky_contact, files, rho, person_type, 
                                              patient_list = output_folder + f'/active_{person_type}.csv',
                                              output_folder = output_folder)
# Find the first layer (including warmup points)
utils.deliver_risky_person(risky_contact, add_patients = False, status = None,
                           file_name = output_folder + '/active_1st_layer.csv', subset = None)
# deliver the risky person list (subset exclude warmup but include patient)
utils.deliver_risky_person(risky_contact, add_patients = True, status = None, 
                           subset = 'app', file_name = output_folder + '/risky_ids.csv',
                           index_name = prefix + 'most_recent_risky_ids',
                           host_url= HOST_URL, port  = PORT, n_thread = n_thread)
