import os
import sys
from functools import partial
from glob import glob
from multiprocessing import Pool
from time import time
from datetime import datetime
from geopy.distance import geodesic

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
   
# remapping the dataframe
def remapping(df, origin, new):
    for key, col in MAPPING['track.person'][origin].items():
        # if find matched ontology
        if (col in df.columns) and (key in MAPPING['track.person'][new]):
            # rename column
            df = df.rename(columns ={col: MAPPING['track.person'][new][key]})
    return df

# retrieve active patient
def retrieve_active_patients(patient_file, person_type, input_folder):
    id_col = MAPPING['track.person'][person_type]['id']
    df = pd.read_csv(patient_file)
    files = [f"{input_folder}/{row[id_col]}.csv" for _, row in df.iterrows() if row['has_track']]
    return files

# concat all file results (support list of df or folder string)
def concat_files(input_obj, order_key = None):
    # if input is a folder
    if type(input_obj) == str:
        dfs = []
        for file_ in glob(input_obj + '/*.csv'):
            dfs.append(pd.read_csv(file_))
    else:
        dfs = input_obj
    # concat list of dfs
    df = pd.concat(dfs, axis = 0, ignore_index = True)
    if order_key:
        return df.sort_values(by = order_key)
    else:
        return df

## ========================== Functions of Contact Model =====================
def p_t_1(row, time):
    latest_start = max(row['sourceTime_min'], row['targetTime_min'])
    earliest_end = min(row['sourceTime_max'], row['targetTime_max'])
    delta = (earliest_end - latest_start) / np.timedelta64(1, 'm')
    # calculat time interval overlap
    overlap = max(0, delta)
    return overlap/time

#compute the probability in space
def p_x(row, R):
    #linear approximation from geodesic
    distance_m=abs(geodesic((row['sourceLat_avg'],row['sourceLong_avg']), 
                            (row['targetLat_avg'],row['targetLong_avg'])).km*1000)
    return 1-(distance_m/(R*2))

# apply probalistic model
def probabilistic_model(_input,_time,R,model='continuos'):
    # if there is no tracking
    try:
        df =pd.read_csv(_input)
    # early stop if the patient did not have track
    except FileNotFoundError:
        return pd.DataFrame()
    # singleton, only self contact
    if (df['sourceId'] != df['targetId']).sum() < 1:
        return pd.DataFrame()
    
    # if there is valid contact outside
    # convert time stamp
    for col in ['sourceTime','targetTime_min','targetTime_max']:
        df[col] = pd.to_datetime(df[col])
        
    # Get External link
    df_other = df[df['sourceId'] != df['targetId']]
 
    # get self link subset
    cols = ['sourceRefId','targetTime_min','targetTime_max','targetLat_avg','targetLong_avg']
    df_self = df[df['sourceId'] == df['targetId']][cols]
    # for selflink target == source
    df_self = df_self.rename(columns ={'targetTime_min': 'sourceTime_min',
                                       'targetTime_max': 'sourceTime_max',
                                       'targetLat_avg' : 'sourceLat_avg',
                                       'targetLong_avg': 'sourceLong_avg'})

    # append selflink info
    df_clean = df_other.merge(df_self, on = 'sourceRefId', how = 'inner')
    # apply probability calculation
    pt = df_clean.apply(p_t_1, time = _time, axis = 1)
    px = df_clean.apply(p_x, R = R, axis = 1)
    df_clean['p'] = pt * px
    # only return the positive overlap contact
    df_clean = df_clean[df_clean['p'] > 0]
    # resturn
    return df_clean

#here we compute the iterative polarization for th risky people
def recursive(p):
    p_f=0
    for _p in p:
        p_f +=_p*(1-p_f)
    return p_f

# Perform risj calculatio
def recursive_p(data,rho):
    df = data.groupby(['sourceId','sourceDataSrc','targetId','targetDataSrc']).agg({'p':recursive,
                                                                          'sourceTime': min})
    # return only risky person
    df =  df.reset_index()
    return df[df['p'] >= rho]

# calculate risky contacts
def calculate_risky_contact(risky_contact, files, rho, person_type,  patient_list = None, output_folder = None):
    if output_folder:
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        risky_contact.drop_duplicates().to_csv(output_folder + '/risky_contacts.csv', index = False)
    # get the list of risky contact
    risky_contact = recursive_p(risky_contact, rho=rho)
    # remove list of known patient
    if patient_list:
        id_p = MAPPING['track.person'][person_type]['id']
        t_p = MAPPING['track.person'][person_type]['time']
        patient_list = pd.read_csv(patient_list)
        risky_contact = risky_contact.merge(patient_list[[id_p,t_p]], left_on = 'targetId', right_on = id_p, how = 'left')
        if t_p + '_y' in risky_contact.columns:
            risky_contact = risky_contact[risky_contact[t_p + '_y'].isna()].drop(columns = t_p + '_y') # if the column name is overlapped
            risky_contact = risky_contact.rename(columns = {t_p + '_x': t_p}) # restore column names
        else:
            risky_contact = risky_contact[risky_contact[t_p].isna()]
    # ouput model result
    sources = len(files)
    contacts = len(pd.unique(risky_contact['targetId']))
    R = contacts / sources
    print(f'Rho {rho}: # of contacts: {contacts}, # of sources: {sources}, R note= {R: .2f}')
    return risky_contact

def reset_es_index(host_url, port, index_type = 'risky_id',index_name = 'most_recent_risky_ids'):
    # this is use to create request body
    settings = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    # mapping of index
    maps = {}
    maps['risky_ids'] = {
        "properties": { 
            'mobileId' :{"type":"keyword"},
            'earliestContactTime' : {"type" : "date"},
            "status": {"type": "integer"},
        }
    }
    maps['red_zones'] = {
        "properties": {
            "mobileId" : {"type":"keyword"},
            "maxTime" :  {"type": "date"},
            "minTime" :  {"type": "date"},
            "location" : {"type" : "geo_point"},
            "delta" : {"type": "integer"}
        }
    }
    # connect to elastic search
    es = Elasticsearch([host_url +':'+ port],timeout=600)
    # Reconstruct index if it is overwrite
    if es.indices.exists(index_name):
        es.indices.delete(index = index_name)
    es.indices.create(index = index_name, body = settings)
    es.indices.put_mapping(index = index_name,body= maps[index_type])
    return Espandas(es)
    
# result dilivery
def deliver_risky_person(risky_contact, status, file_name = None, subset = None, index_name = None,
                         host_url = 'localhost', port = '9200', n_thread = 1):
    if subset:
        # Push notificationconsider only app users for risky result delivery
        risky_contact= risky_contact.loc[risky_contact['targetDataSrc']==subset]
    # condense risky contact
    risky_person = risky_contact.drop_duplicates().groupby('targetId').agg({'sourceTime': 'min','p':'max'})
    risky_person = risky_person.reset_index()
    # save to risky id indexes
    id_c = MAPPING['track.person']['1st_layer']['id']
    t_c = MAPPING['track.person']['1st_layer']['time']
    risky_person = risky_person.rename(columns = {'targetId':id_c, 'sourceTime':t_c})
    # attach status
    if status:
        risky_person['status'] = status
    else:
        risky_person['status'] = 1 + (risky_person['p'] > 0.9).astype(int)
    # save files
    if file_name:
        # save the file
        risky_person.to_csv(file_name, index = False)
    if index_name:
        start_time = time()
        esp = reset_es_index(host_url, port, index_type = 'risky_ids',index_name = index_name)
        # exporting to elastic search
        esp.es_write(risky_person, index_name,thread_count = n_thread)
        # exporting
        print(f'Index: {index_name} Time Lapsed: {(time() - start_time)/60 :.2f} min', flush = True)
        # safely close session
        esp.client.transport.connection_pool.close()

### ============================ function to calculate the read zone =============================

def infected_zones(file_, R = 8):
    Data=pd.read_csv(file_) #read file
    # convert and sort date time
    Data['minTime'] = pd.to_datetime(Data['acquisitionTime'])
    Data['maxTime'] = Data['minTime']
    start_smooth = True
    red_zones = Data # initiate
    # use a while statement aggregate neighbor segments divided by outliers
    while (len(red_zones) != len(Data)) | start_smooth:
        start_smooth = False
        start_run = True
        Data = red_zones.sort_values('minTime')
        red_zones = [] # reset
        # use simple stack / queue to aggregate points
        for _, row in Data.iterrows():
            if start_run: # begin of running
                lat,lng,min_t,max_t =[row['lat']],[row['long']],[row['minTime']],[row['maxTime']]
                x_0=(row['lat'],row['long'])
                start_run = False
            # smaller than diameter 2R, then add segment
            elif geodesic(x_0, (row['lat'],row['long'])).km*1000 < 2*R:
                lat.append(row['lat'])
                lng.append(row['long'])
                min_t.append(row['minTime'])
                max_t.append(row['maxTime'])
                # update dynamic center
                x_0 = (np.mean(lat), np.mean(lng))
            else: # end segment
                # exit and pop all values
                red_zones.append([row['mobileId'],max(max_t), min(min_t), x_0[0], x_0[1]])
                # restart the date we want
                lat,lng,min_t,max_t =[row['lat']],[row['long']],[row['minTime']],[row['maxTime']]
                x_0 = (row['lat'],row['long'])
        # append any remaining queue
        if len(max_t) > 0:
            red_zones.append([row['mobileId'],max(max_t), min(min_t), x_0[0], x_0[1]])
        # organized to redzones
        red_zones = pd.DataFrame(red_zones,columns=['mobileId','maxTime','minTime','lat','long'])
        red_zones = red_zones[red_zones['maxTime'] > red_zones['minTime']]
        red_zones['delta'] = (red_zones['maxTime'] - red_zones['minTime'])/ np.timedelta64(1, 's')
    return red_zones

# run the detectio of red zones
def detect_red_zones(contact_file, person_type, input_folder, R = 8):
    start_time = time()
    # filter with only active contact
    files = retrieve_active_patients(contact_file,person_type, input_folder)
    # for red zone, we consider the patient and contact from all resources.
    with Pool(12) as p:
        dfs = list(p.map(partial(infected_zones, R = R), files))
    df = concat_files(dfs)
    print(f'Get {len(df)} records, Time Lapsed: {(time()-start_time)/60:.2f}min')
    return df.drop_duplicates()

def deliver_red_zone(red_zones, file_name = None, index_name = None, 
                     host_url = 'localhost', port = '9200', n_thread = 1):
    # save files
    if file_name:
        # save the file
        red_zones.to_csv(file_name, index = False)
    if index_name:
        start_time = time()
        # generate geo points
        red_zones['location'] = red_zones.apply(lambda row: [row['long'], row['lat']], axis = 1)
        red_zones = red_zones.drop(columns = ['lat','long'])
        # reset index
        esp = reset_es_index(host_url, port, index_type ='red_zones', index_name = index_name)
        # exporting to elastic search
        esp.es_write(red_zones, index_name,thread_count = n_thread)
        # exporting
        print(f'Index: {index_name} Time Lapsed: {(time() - start_time)/60 :.2f} min', flush = True)
        # safely close session
        esp.client.transport.connection_pool.close()
    
## ==================== Select subset of patient from close contact list ===================

# join close contact table one by one
def filter_close_contact(file_, patient_list, id_col, how = 'inner'):
    print(f'joining: {file_}')
    try:
        df = pd.read_csv(file_)
    except FileNotFoundError:
        print(f'{os.path.basename(file_)} have no contact, skip')
        return pd.DataFrame()
    # select patient subset 
    if how == 'comp': #(compliment)
        df = df.merge(patient_list[id_col,'core'], left_on = 'targetId', right_on = id_col,how = 'left')
        df = df[df['core'].isna()].drop(columns = ['core'])
    else: # other mode
        df = df.merge(patient_list[id_col], left_on = 'targetId', right_on = id_col,how = how)
    # detach duplicate columns
    if id_col != 'targetId':
        df = df.drop(columns = [id_col])
    return df

# get the close contact subsets
def select_close_contact_subset(input_file, person_type, query_files, how = 'inner', n_workers = 4):
    if type(input_file) == str:
        patient_list = pd.read_csv(input_file) # read patient list
    else:
        patient_list = input_file
    id_col = MAPPING['track.person'][person_type]['id']
    func = partial(filter_close_contact, patient_list = patient_list, id_col = id_col, how = how)
    # usin parallel for processing
    with Pool(n_workers) as p:
        dfs = list(p.map(func, query_files))
    df = pd.concat(dfs).sort_values(['sourceId','sourceTime','targetTime'])
    return df.drop_duplicates()