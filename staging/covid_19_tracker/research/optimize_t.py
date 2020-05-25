import yaml
import os
import sys
import pandas as pd
from functools import partial
from multiprocessing import Pool

# add the directory
sys.path.append(os.path.dirname(__file__) + '/..')
# read the relavant libaries
import lib.lib_tracking as utils_t
import lib.lib_close_contact as utils_c
import lib.lib_model as utils_m

# read parameter for the step
with open(os.path.dirname(__file__) + '/../config_params.yaml','r') as f:
    params = yaml.safe_load(f)
    # elasticserch related params
    HOST_URL = params['elasticsearch']['host']
    PORT = params['elasticsearch']['port']
    # for tracking
    d_before =   params['track.person']['day.before']
    d_after =    params['track.person']['day.after']
    lookup_day =   params['track.person']['lookup.day'] # latest date
    # for close contact
    filter_rule = params['track.contact']['filter.rule']
    m_before = params['track.contact']['minute.before']
    m_after =  params['track.contact']['minute.after']
    max_d =    params['track.contact']['distance.max']
    num_cores =params['track.contact']['num.cores']
    rho     =  params['contact.model']['rho']
    # I/O location
    n_core_t  = params['track.person']['num.cores']
    n_core_c = params['track.contact']['num.cores']
    person_type = params['track.person']['person.type'] # read the person you want to track
    in_file =    params[person_type]['input.file']
    track_folder = params[person_type]['folders']['track']
    contact_folder = params[person_type]['folders']['contact']
    profile_folder = params[person_type]['folders']['contact.profile']
    contact_track_folder = params[person_type]['folders']['track.contact']
    deliver_folder = params[person_type]['folders']['deliver']
    max_chunk = 5
    init_chunk = 0

# get the clean list of person
print(f'running query for "{person_type}"')

k = 0
chunk_size = []
# =======================Part 1: Tracking =========================#

# processing tracking df
print(f'running query for level {k}"')
active_patients = utils_t.processing_track_df(in_file, person_type, 
                                        days_before = d_before, 
                                        days_after = d_after,
                                        lookup_day= lookup_day,
                                        prefix = 'fortaleza_')

# querying elasticsearch for track and output to files
active_patients = utils_t.track_persons(active_patients,output_folder = track_folder, 
                                    host_url = HOST_URL, port = PORT, num_cores = n_core_c)
# save active patients actually this one should be the same as original if k is not 0
active_patients = utils_t.save_active_list(active_patients, deliver_folder, person_type, file_name = f'active_chunk_0')
print('T \t 25% p \t 50% p \t 75% p')

for m_after in range(5,6,5):
    # again redundant lines but it is good to keep it. read candidate current chunk
    files = utils_m.retrieve_active_patients(deliver_folder + f'/active_chunk_0.csv', person_type, track_folder)
    # Track the list of close contact from new chunk
    func = partial(utils_c.track_close_contact, output_folder = contact_folder, person_type = person_type, filtering = filter_rule, 
                                            minutes_before = m_before, minutes_after = m_after, distance = max_d,
                                            host_url = HOST_URL, port = PORT, index_prefix = 'fortaleza_')
    # Parrellel processing this chunk to all 
    with Pool(num_cores) as p:
        dfs = list(p.map(func, files))
    # get the available files from next chunk
    files = utils_m.retrieve_active_patients(deliver_folder + f'/active_chunk_0.csv', person_type, contact_folder)
    # shorten the contact from next chunk
    utils_c.shorten_close_contact(files, profile_folder)

    # =======================Part 2: Detect edges =========================#

    # initiate core_id
    active_patients['core'] = True
    id_core = active_patients[['targetId','core']].drop_duplicates()

    # read the determined chunk tracking
    files = utils_m.retrieve_active_patients(deliver_folder + '/active_chunk_0.csv', person_type, profile_folder)
    # assign probability of contact
    with Pool(num_cores) as p:
        func = partial(utils_m.probabilistic_model, _time = m_after, R = max_d,model='continuos')
        result = list(p.map(func, files))
    risky_contact = utils_m.concat_files(result)

    # Recursive to find the edge and wieghts, and thresholding for weights
    edges = utils_m.calculate_risky_contact(risky_contact, files, 0, person_type,
                                            patient_list = None, 
                                            output_folder = deliver_folder + f'/t_{m_after:d}')
    print(f'{m_after} \t {edges["p"].quantile(0.25):.2f} \t {edges["p"].quantile(0.5):.2f} \t {edges["p"].quantile(0.75):.2f}')
    edges.to_csv(deliver_folder + f'/edges_{m_after:d}.csv', index = False)

print('Running Complete')