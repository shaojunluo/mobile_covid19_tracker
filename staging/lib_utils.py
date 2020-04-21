import os
from ast import literal_eval
from functools import partial
from glob import glob
from hashlib import md5
from multiprocessing import Pool
from time import time

import numpy as np
import pandas as pd
import yaml
from elasticsearch import Elasticsearch

from lib_espandas import Espandas

# # Reserve for development
# from importlib import reload
# import lib_espandas
# reload(lib_espandas)
# from lib_espandas import Espandas

# global executer
with open('config_ontology.yaml','r') as f:
    # raw data ontology
    MAPPING = yaml.safe_load(f)

### ================================Utiles for data ingestion  =======================###

# create hash id
def hash_record(row):
    hash_str = ''
    for v in row:
        try:
            hash_str += str(v)
        except:
            pass
    return md5(hash_str.encode('utf-8')).hexdigest()

# construct index
# output espandas object for calling.
def construct_index(index_name, host_url= 'http://localhost', port = 9200):
    # this is use to create request body
    settings = {
        "settings" : {
            "number_of_shards": 5,
            "number_of_replicas": 0
        }
    }

    es_mapping = {
        "properties": { 
            "mobileId" :{"type":"keyword"},
            "acquisitionTime" : {"type" : "date"},
            "location" : {"type" : "geo_point"},
            "movingRate": {"type": "double"}
        }
    }
    # connect to elastic search
    es = Elasticsearch([host_url +':'+ port],timeout=600)
    # Reconstruct index
    if es.indices.exists(index_name):
        es.indices.delete(index = index_name)
    es.indices.create(index = index_name, body = settings)
    # PUT MAPPING IN THE INDEX
    es.indices.put_mapping(index = index_name,body= es_mapping)
    # call espandas objects
    esp = Espandas(es)
    return esp

# get index name (subject to change according to file name)
def get_index_name(file_, prefix = ''):
    name = os.path.basename(file_).split('.')[0]
    index_name = pd.to_datetime(name, format = '%b_%d').strftime('%m_%d')
    return prefix + index_name

# calculate derivativation of uneven time interval
def uneven_derivative(df_sorted, col, t_col, group_col = None):
    # calculate dx/dt
    # see paper here https://www.tandfonline.com/doi/pdf/10.3402/tellusa.v22i1.10155
    if group_col is None:
        dt = df_sorted[t_col].diff(1).dt.seconds.fillna(1)
        dx_p = df_sorted[col].diff(1).fillna(0)
        dx_n = df_sorted[col].diff(-1).fillna(0)
    else:
        dt= df_sorted.groupby(group_col)[t_col].diff(1).dt.seconds.fillna(1)
        dx_p = df_sorted.groupby(group_col)[col].diff(1).fillna(0)
        dx_n = df_sorted.groupby(group_col)[col].diff(-1).fillna(0)
    dt[dt == 0] = 1 # no inf value for velosity
    dt_p = dt.shift(1, fill_value = 1) # positive step interval
    dt_n = dt.shift(-1, fill_value = 1) # negetive step interval
    vx = (dx_p*dt_n*dt_n + dx_n*dt_p*dt_p)/(dt_p*dt_n*(dt_p + dt_n)) # difference
    return vx

# calculate velocity
def add_moving_rate(df, moving_rate_col, lat_col, lon_col, t_col, group_col = None):
    R = 6.3781e6 # earth radius
    if group_col is None:
        df_sorted = df.sort_values(t_col)
    else:
        df_sorted = df.sort_values([group_col,t_col])
    # calculate the change rate of lat lon
    v_lat = uneven_derivative(df_sorted, lat_col, t_col, group_col = group_col)
    v_lon = uneven_derivative(df_sorted, lon_col, t_col, group_col = group_col)
    # calculate actual dy, dx
    vy = R * v_lat.fillna(0)
    vx = R * np.cos(df_sorted[lat_col]/180*np.pi) * v_lon.fillna(0)
    df_sorted[moving_rate_col] = (vx*vx + vy*vy).pow(1/2)
    # return sorted dataframe
    return df_sorted

# processing file for Elasticsearch
def process_for_ES(file_):
    # read MAPPING
    id_col = MAPPING['ingestion']['id']
    t_col = MAPPING['ingestion']['time'] 
    lat_col = MAPPING['ingestion']['latitude']
    long_col = MAPPING['ingestion']['longitude']
    
    # processing dataframes
    df = pd.read_csv(file_) # read file
    df = df.set_index('Unnamed: 0') # reset index
    
    # create unique uid (must dedup to avoid hash collision)
    df['reference_id'] = df.apply(lambda row: hash_record(row),axis = 1)
    df = df.drop_duplicates(subset = 'reference_id')
    # transfer to time stamp
    df[t_col] = pd.to_datetime(df[t_col],unit ='s',utc = True) # transfer time stamp
    df[t_col] = df[t_col].dt.tz_convert('America/Fortaleza') # convert time zone
    
    # add velocity guess and sort table
    df = add_moving_rate(df, 'movingRate', 
                         lat_col, long_col, t_col, 
                         group_col = id_col)
    # reformatting location
    df['location'] = df.apply(lambda row: [row[lat_col], row[long_col]], axis = 1)
    
    # final cleaning
    df = df.rename(columns = {id_col: 'mobileId', t_col: 'acquisitionTime'}) # rename columns
    return df[['reference_id','mobileId', 'acquisitionTime', 'location','movingRate']] # return subset


# read files to elasticsearch 
def read_to_elastic(file_, host_url = 'http://localhost', port ='9200', n_thread = 1, prefix = ''):
    start_time = time()
    # get index name
    index_name = get_index_name(file_, prefix = prefix)
    # creating index
    esp = construct_index(index_name, host_url= host_url, port = port)
    # covert dataframes
    df = process_for_ES(file_)
    # exporting to elastic search
    esp.es_write(df, index_name, uid_name = 'reference_id', geo_col_dict= None, thread_count = n_thread)
    # exporting
    print(f'Index {index_name} Time Lapsed: {(time() - start_time)/60 :.2f} min', flush = True)
    # safely close session
    esp.client.transport.connection_pool.close()
    return index_name

# add the list of ingested index
def add_ingested_index(index_list):
    # read
    with open('config_es_index.yaml', 'r') as stream:
        params = yaml.safe_load(stream)
    # mutate
    old_list = params['index.ingested']
    new_list = sorted(list(set(old_list + index_list)))
    params['index.ingested'] = new_list
    
    # write
    with open('config_es_index.yaml','w') as stream:
        yaml.safe_dump(params, stream)


### =================== utiles for track patient ================== ##

# the following algorithm subject to change if the name of index use different way
def get_query_index(sympton_date, index_list, days_before = 15, days_after = 15, prefix = ''):
    # calculate date range
    day_start = sympton_date - pd.Timedelta(days = days_before)
    day_end = sympton_date + pd.Timedelta(days = days_after)
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
def build_track(es, row, output_folder = 'patient_track'):
    # create folfer 
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
     
    records = []
    # pending query result
    for idx in row['index_list']:
        records += query_track(es, row['mobileId'], idx)
    
    # manipulate the result
    df =pd.DataFrame(records)                  # construct new dataframes
    if len(df) == 0:
        print(f"ID {row['mobileId']} out of scope, skip",flush = True)
    else:
        df['acquisitionTime'] = pd.to_datetime(df['acquisitionTime'])
        df = df.sort_values('acquisitionTime')     # sort by time
        df['lat'] = df['location'].apply(lambda x: x[0])
        df['long'] = df['location'].apply(lambda x: x[1])
        df = df.drop(columns = 'location')
        # save result
        df.to_csv(output_folder + '/' + row['mobileId'] +'.csv', index = False)

# processing the df for person to track
def processing_track_df(input_file, person_type, days_before = 15, days_after = 15, prefix = ''):
    # read MAPPING
    id_col = MAPPING['track.person'][person_type]['id']
    date_col = MAPPING['track.person'][person_type]['date']
    
    # processing
    df = pd.read_csv(input_file) # read file
    df = df.rename(columns ={id_col: 'mobileId'}) # align ontoligy
    df[date_col] = pd.to_datetime(df[date_col]) 
    # get the list of index we can query
   # get the available index
    with open('config_es_index.yaml','r') as stream:
        index_list = yaml.safe_load(stream)['index.ingested']
    func = lambda x: get_query_index(x, index_list, days_before= days_before, days_after= days_after, prefix = prefix)
    df['index_list'] = df[date_col].apply(func)
    return df

# track a list of persons
def track_persons(query_df, output_folder, host_url = 'http://localhost', port = "9200"):
    start_time = time()
    print(f"Tracking: {len(query_df)} persons",flush = True)
    # tracking the trace of every person. Don't need any parallel becasue it is fast in general.
    es = Elasticsearch([host_url +':'+ port],timeout=600)
    query_df.apply(lambda row: build_track(es, row, output_folder= output_folder), axis = 1)
    es.transport.connection_pool.close()
    print(f'Final available IDs: {len(glob(output_folder + "/*.csv"))}, Time Lapse {(time() - start_time)/60:.2f}min',flush = True)

### =================== utiles for close contact ================== ##        

# query the track within time frame
def time_query(start_time,end_time):
    body = { "range" : {
                "acquisitionTime" : {
                    "gte": start_time, 
                    "lte": end_time,
                }
            }
        }
    return body

# query within space distance
def space_query(lat, lon, distance):
    body = {"must" : {"match_all":{}},
            "filter" : {
                "geo_distance" : {
                    "distance" : distance,
                    "location" : [lat, lon]
                    }
                }
            }
    return body

# perform the combined queries
def combine_queries(time_queries, space_queries, query_id, size):
    body = {"size" : size,
            "query": {
                "bool": {
                    "must_not" : {
                            "term" : { "mobileId" : query_id }
                    },
                    "must": [
                        time_queries,
                        {"bool": space_queries}
                    ]
                }
            }
        }
    return body

# generate concatct records
def generate_contact_record(hit, row):
    result = {}
    
    result['sourceRefId'] = row['reference_id']
    result['sourceId'] = row['mobileId']
    result['sourceTime'] = row['acquisitionTime']
    result['sourceLat'] = row['lat']
    result['sourceLong'] = row['long']
    result['sourceMovingRate'] = row['movingRate']
    
    result['targetRefId'] = hit['_id']
    result['targetId'] = hit['_source']['mobileId']
    result['targetTime'] = hit['_source']['acquisitionTime']
    result['targetLat'] = hit['_source']['location'][0]
    result['targetLong'] = hit['_source']['location'][1]
    result['targetMovingRate'] = hit['_source']['movingRate']
    
    return result

# generate self-concatct records
def generate_self_link(row):
    self_link = {}
    self_link['sourceRefId'] = row['reference_id']
    self_link['sourceId'] = row['mobileId']
    self_link['sourceTime'] = row['acquisitionTime']
    self_link['sourceLat'] = row['lat']
    self_link['sourceLong'] = row['long']
    self_link['sourceMovingRate'] = row['movingRate']
    
    self_link['targetRefId'] = row['reference_id']
    self_link['targetId'] = row['mobileId']
    self_link['targetTime'] = row['acquisitionTime']
    self_link['targetLat'] = row['lat']
    self_link['targetLong'] = row['long']
    self_link['targetMovingRate'] = row['movingRate']
    return self_link

# get close contact of 1 point
def get_close_contact(es, row, d = '10m', index_prefix = '', self_link = True):
    index_name = index_prefix + row['acquisitionTime'].strftime('%m_%d').lower() # get the index time to query!
    time_queries = time_query(row['startTime'], row['endTime']) # prepare time query
    space_queries = space_query(row['lat'], row['long'], distance = d) # prepare space query
    body = combine_queries(time_queries, space_queries, row['mobileId'], size = 10000) # combine queries
    result = es.search(index = index_name, body = body) # query
    records = [generate_contact_record(hit, row) for hit in result['hits']['hits']] # generate records
    # attach self link if required
    if self_link:
        records.append(generate_self_link(row))
    # process valid hits
    return records

# generate summary file
def agg_close_contacts(df):
    d = {}
    d['hits'] = len(df)
    d['med_targetMovingRate'] = df['targetMovingRate'].median()
    d['med_sourceMovingRate'] = df['sourceMovingRate'].median()
    return pd.Series(d)

# track close contact for one patient and output to files
def track_close_contact(file_, output_folder, minutes_before = 3, minutes_after = 3, distance = '10m', 
                        host_url = 'http://localhost', port = '9200', index_prefix = '',self_link = True):
    start_time = time()
    # create dir if not exist
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    es = Elasticsearch([host_url +':'+ port],timeout=600) # setup es connection
    
    # reading and processing tables
    df = pd.read_csv(file_)
    df['acquisitionTime'] = pd.to_datetime(df['acquisitionTime'])
    # get the neighbor time
    df['startTime'] = (df['acquisitionTime'] - pd.Timedelta(minutes = minutes_before)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    df['endTime'] = (df['acquisitionTime'] + pd.Timedelta(minutes = minutes_after)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    # apply to every row and then concat to final close contact table
    close_contact = pd.DataFrame(df.apply(lambda row: get_close_contact(es, row, d = distance, 
                                                                        index_prefix = index_prefix,
                                                                        self_link = self_link),axis = 1).sum())
    # safely close connection
    es.transport.connection_pool.close()
    # if it is null result, the early return
    if len(close_contact) == 0:
        print(f'File {os.path.basename(file_)}: No contacts. Time Lapsed: {(time()-start_time)/60:.2f}min', flush = True)
        return close_contact
    # sort values
    close_contact = close_contact.sort_values(['sourceTime','targetTime'])
    # save result
    close_contact.to_csv(output_folder +'/'+ os.path.basename(file_), index = False)
    
    print(f'File {os.path.basename(file_)}: time Lapsed: {(time()-start_time)/60:.2f}min', flush = True)
    # get the summary of close contact
    close_contact_summary = close_contact.groupby('targetId').apply(agg_close_contacts)
    close_contact_summary['sourceId'] = os.path.basename(file_).split('.')[0]
    return close_contact_summary.reset_index()

# write close contact summary
def close_contact_summary(dfs, result_folder):
    print('Generating Summary')
    if not os.path.exists(result_folder):
        os.mkdir(result_folder)
    # contact list
    df = pd.concat(dfs, ignore_index = True)
    # save result
    df.to_csv(result_folder + '/close_contact_summary.csv', index = False)

## ==================== Select subset of patient from close contact list ===================

# join close contact table one by one
def join_close_contact(file_, patient_list, id_col):
    print(f'joining: {file_}')
    df = pd.read_csv(file_)
    # select patient subset
    df = df.merge(patient_list[id_col], left_on = 'targetId', right_on = id_col,how = 'inner')
    df = df.drop(columns = [id_col])
    return df

# get the close contact subsets
def select_close_contact_subset(input_file, person_type, query_files, n_workers = 4):
    patient_list = pd.read_csv(input_file) # read patient list
    id_col = MAPPING['track.person'][person_type]['id']
    func = partial(join_close_contact, patient_list = patient_list, id_col = id_col)
    # usin parallel for processing
    with Pool(n_workers) as p:
        dfs = list(p.map(func, query_files))
    df = pd.concat(dfs).sort_values(['sourceId','sourceTime','targetTime'])
        
    return df.drop_duplicates()
    

