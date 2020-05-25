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
    lookup_day = params['track.person']['lookup.day'] # latest date
    # for close contact
    filter_rule = params['track.contact']['filter.rule']
    m_before = params['track.contact']['minute.before']
    m_after =  params['track.contact']['minute.after']
    max_d =    params['track.contact']['distance.max']
    num_cores= params['track.contact']['num.cores']
    rho =      params['contact.model']['rho']
    # I/O location
    n_core_t = params['track.person']['num.cores']
    n_core_c = params['track.contact']['num.cores']
    person_type = params['track.person']['person.type'] # read the person you want to track
    in_file =     params[person_type]['input.file']
    track_folder = params[person_type]['folders']['track']
    contact_folder = params[person_type]['folders']['contact']
    profile_folder = params[person_type]['folders']['contact.profile']
    contact_track_folder = params[person_type]['folders']['track.contact']
    deliver_folder = params[person_type]['folders']['deliver']
    max_chunk = 25
    init_chunk = 1
    skip = False

print(f'Fay before: {d_before}, day after {d_after}')
print(f'ds {max_d}, min before {m_before}, max after {m_after}')
print(f'rho {rho}, lookup day {lookup_day}')
# helper function for final way of cleanining
def undirectize(row):
    if row['sourceId'] < row['targetId']:
        return row['sourceId'] + '-' + row['targetId']
    else:
        return row['targetId'] + '-' + row['sourceId']

# start expanding network from init chunk
for k in range(init_chunk, max_chunk):

    # =======================Part 1: Tracking =========================#
    if k == 0:
        d_b = d_before
        d_f = d_after
    else:
        # change input file
        in_file = deliver_folder + f'/active_chunk_{k}.csv' 
        d_b = 0
        d_f = d_before + d_after
    # processing tracking df
    print(f'running query for level {k}')
    active_patients = utils_t.processing_track_df(in_file, person_type, 
                                            days_before = d_b, 
                                            days_after = d_f,
                                            lookup_day= lookup_day,
                                            prefix = 'fortaleza_')

    # querying elasticsearch for track and output to files
    active_patients = utils_t.track_persons(active_patients,output_folder = track_folder, 
                                        host_url = HOST_URL, port = PORT, num_cores = n_core_c)
    # save active patients actually this one should be the same as original if k is not 0
    active_patients = utils_t.save_active_list(active_patients, deliver_folder, person_type, file_name = f'active_chunk_{k}')
    # again redundant lines but it is good to keep it. read candidate current chunk
    files = utils_m.retrieve_active_patients(deliver_folder + f'/active_chunk_{k}.csv', person_type, track_folder)
    # Track the list of close contact from new chunk
    func = partial(utils_c.track_close_contact, output_folder = contact_folder, person_type = person_type, filtering = filter_rule, 
                                            minutes_before = m_before, minutes_after = m_after, distance = max_d,
                                            host_url = HOST_URL, port = PORT, index_prefix = 'fortaleza_')
    # Parrellel processing this chunk to all 
    with Pool(num_cores) as p:
        dfs = list(p.map(func, files))
        
    # initiate edges and cores:
    if k == 0:
        # initiate core_id
        active_patients['core'] = True
        id_core = active_patients[['sourceId','core']].drop_duplicates()
        # contacts
        edge_list = pd.DataFrame(columns = ['sourceId','sourceDataSrc','targetId','targetDataSrc','sourceTime', 'p'])
    # if not init layer
    else:
        edge_list = pd.read_csv(deliver_folder + '/final_edges.csv',parse_dates = ['sourceTime'])
        id_core = pd.read_csv(deliver_folder + '/core.csv')
    
    # get the available files from next chunk
    files = utils_m.retrieve_active_patients(deliver_folder + f'/active_chunk_{k}.csv', person_type, contact_folder)
    # get the list of contact to new candidate users
    contacts = utils_m.select_close_contact_subset(id_core, person_type, files, how = 'comp', n_workers = 12)
    candidate_ids = contacts[['sourceId','targetId']].drop_duplicates()
    # new candidate
    active_patients = active_patients.rename(columns = {'targetId':'sourceId'})
    candidate_ids.join(active_patients, on = 'sourceId',how = 'inner')

    

    
    # shorten the contact from next chunk
    utils_c.shorten_close_contact(files, profile_folder)
    # =======================Part 2: Detect candidates =========================#
        
    # read the determined chunk tracking
    files = utils_m.retrieve_active_patients(deliver_folder + f'/active_chunk_{k:d}.csv', person_type, profile_folder)
    # assign probability of contact
    with Pool(num_cores) as p:
        func = partial(utils_m.probabilistic_model, _time = m_after, R = max_d,model='continuos')
        result = list(p.map(func, files))
    risky_contact = utils_m.concat_files(result)
    
    # Recursive to find the edge and wieghts, and thresholding for weights
    edges = utils_m.calculate_risky_contact(risky_contact, files, rho, person_type,
                                            patient_list = None, 
                                            output_folder = deliver_folder + f'/chunk_{k:d}')
    # Append trust edges 
    edge_list = edge_list.append(edges, sort = False, ignore_index = True)
    # create and groupby undirected edges
    edge_list['undirected'] = edge_list.apply(lambda row: undirectize(row), axis = 1)
    edge_list = edge_list.groupby('undirected').agg({'p':max, 'sourceTime': min}).reset_index()
    # regain source and target
    st = edge_list['undirected'].apply(lambda x: pd.Series({'sourceId': x.split('-')[0], 'targetId': x.split('-')[1]}))
    edge_list = pd.concat([edge_list, st], axis = 1).drop(columns = ['undirected'])
    # save edge_list
    edge_list.to_csv(deliver_folder + '/final_edges.csv', index = False)
    
    #============================Part 3: find condidates ========================#
    
    # get the candidate:
    edges['candidate'] = True
    candidates = edges[['targetId','sourceTime','candidate']].merge(id_core, on = 'targetId', how = 'outer').drop_duplicates()
    # If the target is not in core, save to next chunk
    new_chunk = candidates[candidates['core'].isna()]
    new_chunk.to_csv(deliver_folder + f'/active_chunk_{(k+1):d}.csv', index = False)
    # output
    print(f"New Chunk {len(new_chunk)}, Enriched link {len(candidates[~(candidates['core'].isna() | candidates['candidate'].isna())])}")
    # update core ids
    candidates['core'] = True
    id_core = candidates[['targetId','core']].drop_duplicates().rename(columns = {'tragetId':'sourceId'})
    id_core.to_csv(deliver_folder + '/core.csv',index = False)
    
    if len(new_chunk) == 0:
        print('No nodes in new chunk, stop early')
        break

print('Running Complete')