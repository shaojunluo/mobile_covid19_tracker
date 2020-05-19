import os
import sys
from ast import literal_eval
from time import time
from glob import glob
from multiprocessing import Pool
from functools import partial

import numpy as np
import pandas as pd
import yaml
from elasticsearch import Elasticsearch

# global executer
with open(os.path.dirname(__file__) + '/../config_ontology.yaml','r') as f:
    # raw data ontology
    MAPPING = yaml.safe_load(f)
    
### =================== utiles for track patient ================== ##

# get the index name to query
# the following algorithm subject to change if the name of index use different way
def get_query_index(sympton_date, index_list, days_before = 15, days_after = 15, status = True, dead_time = None, prefix = ''):
    # calculate date range
    day_start = sympton_date - pd.Timedelta(days = days_before)
    # if the patient didn't make it, we track until death
    if dead_time != '':
        day_end = pd.to_datetime(dead_time)
    else:
        day_end = sympton_date + int(status)*pd.Timedelta(days = days_after)
    # get the day range of query
    day_range = pd.date_range(day_start,day_end)
    # return the index within range
    full_list = [prefix + day.strftime('%m_%d') for day in day_range]
    return [idx for idx in full_list if (idx in index_list)]

# query the track of specific person from index
def query_track(es, mobile_id, index_name):
    body = {"size" : 10000,
            "query": {
                    "term" : {
                        "mobileId" : mobile_id
                }
            }
        }
    result = es.search(index = index_name, body = body)
    if len(result['hits']['hits']) == 10000:
        print('return cap 10000 hit', flush = True)
    return [r['_source'] for r in result['hits']['hits']]

# build the track of specific person
def build_track(row, host_url = 'http://localhost',port = '9200', output_folder = 'patient_track'):
    # tracking the trace of every person.
    es = Elasticsearch([host_url +':'+ port],timeout=600)
    # create folfer 
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
     
    records = []
    # pending query result
    for idx in row['index_list']:
        records += query_track(es, row['mobileId'], idx)
    # securely close link
    es.transport.connection_pool.close()
    # manipulate the result
    df = pd.DataFrame(records)  # construct new dataframes
    if len(df) == 0:
        print(f"ID {row['mobileId']} out of scope, skip",flush = True)
        return False
    else:
        # for output attach time zone
        df['acquisitionTime'] = pd.to_datetime(df['acquisitionTime'],utc=True).dt.tz_convert(row['time_zone'])
        df = df.sort_values('acquisitionTime')     # sort by time
        df['lat'] = df['location'].apply(lambda x: x[0])
        df['long'] = df['location'].apply(lambda x: x[1])
        df = df.drop(columns = 'location')
        # save result
        df.to_csv(output_folder + '/' + row['mobileId'] +'.csv', index = False)
        return True

# processing the df for person to track
def processing_track_df(input_file, person_type, days_before = 15, days_after = 15,
                        lookup_day = 'today', prefix = ''):
    # read MAPPING
    id_col = MAPPING['track.person'][person_type]['id']
    date_col = MAPPING['track.person'][person_type]['time']
    time_zone = MAPPING['track.person'][person_type]['timezone'] # read time zone
    
    # processing
    df = pd.read_csv(input_file) # read file
    df = df.rename(columns ={id_col: 'mobileId'}) # align ontoligy
    if 'status' in MAPPING['track.person'][person_type].keys():
        stat_col = MAPPING['track.person'][person_type]['status']
        dead_flag = MAPPING['track.person'][person_type]['flag.dead']
        dead_time = MAPPING['track.person'][person_type]['time.dead']
        if stat_col not in df.columns: # if there is no 
            df[stat_col] = '' #null status
        if dead_time not in df.columns:
            df[dead_time] = '' 
        df[stat_col] =  df[stat_col].fillna('')
        df[dead_time] = df[dead_time].fillna('')
    else:
        stat_col = 'isDead'
        dead_time = 'dead_time'
        df[stat_col] = False
        df[dead_time] = ''
        dead_flag = True
        
    df[date_col] = pd.to_datetime(df[date_col]) # in here because only day present, therefore we don't proceed with utc
    # clean duplication and keep the earlist record
    df_distinct = df.groupby('mobileId')[date_col].min().reset_index()
    print(df_distinct)
    # only get the df with earliest record
    df = df.merge(df_distinct, on = ['mobileId',date_col], how = 'inner')
    print(len(df))
    # determine and filter by lookup day
    if lookup_day == 'today':
        lookup_day = pd.Timestamp.today()
    # for anydates other than none
    if lookup_day != 'none':
        lookup_day = pd.Timestamp(lookup_day)
        # filter the person with recent infection correspond to lookup_day, otherwise consider all.
        # filter long ago-patient
        fil_past = df[date_col].dt.tz_localize(None) > lookup_day.tz_localize(None) - pd.Timedelta(days = days_after)
        fil_future = df[date_col].dt.tz_localize(None) < lookup_day.tz_localize(None) # no patient in the future
        df = df[fil_past & fil_future]
    else:
        print('lookup date is not specified, use all data')
    
    # get the available index
    with open(os.path.dirname(__file__) + '/../config_es_index.yaml','r') as stream:
        index_list = yaml.safe_load(stream)['index.ingested']
    # if the status is not "None" then it is dead, no need to track later days.
    func = lambda x: get_query_index(x[date_col],index_list, days_before= days_before, 
                                                             days_after= days_after,
                                                             status = x[stat_col]!=dead_flag,
                                                             dead_time= x[dead_time],
                                                             prefix = prefix)
    df['index_list'] = df.apply(func, axis = 1)
    df['time_zone'] = time_zone
    return df

# track a list of persons
def track_persons(query_df, output_folder, host_url = 'http://localhost', port = "9200", num_cores = 1):
    start_time = time()
    print(f"Tracking: {query_df.shape[0]} persons",flush = True)
    # split into all rows
    rows = [row for _, row in query_df.iterrows()]
    with Pool(num_cores) as p:
        func = partial(build_track, host_url = host_url,port = port, output_folder= output_folder)
        has_track = list(p.map(func, rows))
    # attch columns
    query_df['has_track'] = has_track 
    print(f'Final trackable IDs: {query_df["has_track"].sum():.0f}, Time Lapse {(time() - start_time)/60:.2f}min',flush = True)
    # return the final query df
    return query_df.drop(columns = 'index_list')

# save the active patients into file
def save_active_list(active_patients, deliver_folder, person_type, file_name = None):
    id_col = MAPPING['track.person'][person_type]['id']
    active_patients = active_patients.rename(columns ={'mobileId':id_col}) # reverse ontoligy
    if not os.path.exists(deliver_folder):
        os.mkdir(deliver_folder)
    # save
    if file_name:
        active_patients.to_csv(deliver_folder + '/' + file_name + '.csv',index =False)
    else:
        active_patients.to_csv(deliver_folder +f'/active_{person_type}.csv',index =False)
    return active_patients