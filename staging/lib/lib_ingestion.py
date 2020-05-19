import os
import sys
from ast import literal_eval
from hashlib import md5
from time import time

import numpy as np
import pandas as pd
import yaml
from elasticsearch import Elasticsearch
# add the path to libary to current dir
sys.path.append(os.path.dirname(__file__))
from espandas import Espandas

# global executer
with open(os.path.dirname(__file__) + '/../config_ontology.yaml','r') as f:
    # raw data ontology
    MAPPING = yaml.safe_load(f)

### ================================Utiles for data ingestion  =======================###

# determine ingestion mode
def ingestion_mode(data_source, default_mode = 'skip'):
    with open(os.path.dirname(__file__) + '/../config_status.yaml') as f:
        status = yaml.safe_load(f)
        t_data = status['last.update'][f'data.{data_source}']
        t_run = status['last.run']
        if pd.Timestamp(t_data) < pd.Timestamp(t_run):
            return 'skip'
        else:
            return default_mode
            
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
def construct_index(index_name, host_url= 'http://localhost', port = 9200, mode = 'overwrite'):
    # this is use to create request body
    settings = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    # mapping of index
    es_mapping = {
        "properties": { 
            "mobileId" :{"type":"keyword"},
            "dataSource": {"type":"keyword"},
            "acquisitionTime" : {"type" : "date"},
            "location" : {"type" : "geo_point"},
            "movingRate": {"type": "double"},
        }
    }
    # connect to elastic search
    es = Elasticsearch([host_url +':'+ port],timeout=600)
    # Reconstruct index if it is overwrite
    if es.indices.exists(index_name):
        if mode == 'overwrite':
            es.indices.delete(index = index_name)
            es.indices.create(index = index_name, body = settings)
            es.indices.put_mapping(index = index_name,body= es_mapping)
        elif mode == 'skip': # if choose skip then we do nothing
            return None
        elif mode != 'append':    # if it is not append, then invalid
            raise ValueError("Invalid Mode, valid mode should be ['overwrite', 'skip', 'append']")
    else:
        es.indices.create(index = index_name, body = settings)       # create indicies
        es.indices.put_mapping(index = index_name,body= es_mapping)  # PUT MAPPING IN THE INDEX
    # call espandas objects
    esp = Espandas(es)
    return esp

# get index name (subject to change according to file name)
def get_index_name(file_, prefix = ''):
    name = os.path.basename(file_).split('.')[0]
    # get date to query, we ignore the misalign of timezone
    index_name = pd.to_datetime(name, format = '%B_%d').strftime('%m_%d')
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
def process_for_ES(file_, data_source):
    # read MAPPING
    id_col = MAPPING['ingestion'][data_source]['id']
    t_col = MAPPING['ingestion'][data_source]['time'] 
    lat_col = MAPPING['ingestion'][data_source]['latitude']
    long_col = MAPPING['ingestion'][data_source]['longitude']
    
    # processing dataframes
    df = pd.read_csv(file_)[[id_col,t_col, lat_col, long_col]]# read file
    df['dataSource'] = data_source
    
    # create unique uid (must dedup to avoid hash collision)
    df['reference_id'] = df.apply(lambda row: hash_record(row),axis = 1)
    df = df.drop_duplicates(subset = 'reference_id')
    # transfer to time stamp, for ingestion NO time zone required
    df[t_col] = pd.to_datetime(df[t_col],unit ='s') # transfer time stamp
    
    # add velocity guess and sort table
    df = add_moving_rate(df, 'movingRate', 
                         lat_col, long_col, t_col, 
                         group_col = id_col)
    # reformatting location
    df['location'] = df.apply(lambda row: [row[lat_col], row[long_col]], axis = 1)
    
    # final cleaning
    df = df.rename(columns = {id_col: 'mobileId', t_col: 'acquisitionTime'}) # rename columns
    return df[['reference_id','dataSource','mobileId','acquisitionTime','location','movingRate']] # return subset

# read files to elasticsearch
def read_to_elastic(file_, data_source, host_url = 'http://localhost', port ='9200', n_thread = 1, mode = 'overwrite', prefix = ''):
    start_time = time()
    # get index name
    index_name = get_index_name(file_, prefix = prefix)
    # creating/update index
    esp = construct_index(index_name, host_url= host_url, port = port, mode = mode)
    if esp is None: # if choose skip and index exist, then early stop
        print(f'Index {index_name} exist, skipped by rule')
        return index_name
    # covert dataframes
    df = process_for_ES(file_,data_source)
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
    with open(os.path.dirname(__file__) + '/../config_es_index.yaml', 'r') as stream:
        params = yaml.safe_load(stream)
    # mutate
    old_list = params['index.ingested']
    new_list = sorted(list(set(old_list + index_list)))
    params['index.ingested'] = new_list
    
    # write
    with open(os.path.dirname(__file__) + '/../config_es_index.yaml','w') as stream:
        yaml.safe_dump(params, stream)
