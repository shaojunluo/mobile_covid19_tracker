import os
import sys

from collections import Counter
from glob import glob
from functools import partial
from multiprocessing import Pool
from time import time

import pandas as pd
import yaml
from elasticsearch import Elasticsearch

# global executer
with open(os.path.dirname(__file__) + '/../config_ontology.yaml','r') as f:
    # raw data ontology
    MAPPING = yaml.safe_load(f)

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
                    "distance" : str(distance) + 'm',
                    "location" : [lat, lon]
                    }
                }
            }
    return body

# perform the combined queries (alow self query)
# to disable self link, add "must_not" : {"term" : { "mobileId" : query_id }},
def combine_queries(time_queries, space_queries, query_id, size, only_self_link = False):
    body = {"size" : size,
            "query": {
                "bool": {
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

# check existence of singleton
def remove_singleton(result, row):
    # count the list of ids
    id_list = [hit['_source']['mobileId'] for hit in result['hits']['hits']]
    id_counter = Counter(id_list)
    # Consider only the point with self record nearby & shows more than twice
    if (len(id_counter) < 2) | (id_counter[row['mobileId']] < 2):
        records = [] # discard singleton, return empty reult
    else:
        # generate valid records
        records = [generate_contact_record(hit, row) for hit in result['hits']['hits'] \
                   if id_counter[hit['_source']['mobileId']] > 1] 
    return records

# get close contact of 1 point
def get_close_contact(es, row, d = '10m', index_prefix = ''):
    index_name = index_prefix + row['acquisitionTime'].strftime('%m_%d').lower() # get the index time to query!
    time_queries = time_query(row['startTime'], row['endTime'] ) # prepare time query
    space_queries = space_query(row['lat'], row['long'], distance = d) # prepare space query
    body = combine_queries(time_queries, space_queries, row['mobileId'], size = 10000) # combine queries
    result = es.search(index = index_name, body = body) # query
    # check single tons from results
    return remove_singleton(result, row)

# generate summary file 
def summary_close_contact(df):
    d = {}
    d['hits'] = len(df)
    d['med_targetMovingRate'] = df['targetMovingRate'].median()
    d['med_sourceMovingRate'] = df['sourceMovingRate'].median()
    return pd.Series(d)

# track close contact for one patient and output to files
def track_close_contact(file_, person_type, output_folder, minutes_before = 3, minutes_after = 3, distance = '10m',
                        host_url = 'http://localhost', port = '9200', index_prefix = ''):
    start_time = time()
    # create dir if not exist
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    es = Elasticsearch([host_url +':'+ port],timeout=600) # setup es connection

    # reading and processing tables
    df = pd.read_csv(file_)
    df['acquisitionTime'] = pd.to_datetime(df['acquisitionTime']).dt.tz_convert(tz = None) # convert to utc and remove timezone info
    # get the neighbor time. (allow 10 second to tolerate query error if minutes = 0)
    df['startTime'] = (df['acquisitionTime'] - pd.Timedelta(minutes = minutes_before)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    df['endTime'] = (df['acquisitionTime'] + pd.Timedelta(minutes = minutes_after)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    # apply to every row and then concat to final close contact table
    close_contact = pd.DataFrame(df.apply(lambda row: get_close_contact(es, row, d = distance, 
                                                                        index_prefix = index_prefix),axis = 1).sum())
    # safely close connection
    es.transport.connection_pool.close()
    # if it is null result, the early return
    if len(close_contact) == 0:
        print(f'File {os.path.basename(file_)}: No contacts. Time Lapsed: {(time()-start_time)/60:.2f}min', flush = True)
        return close_contact
    # get timezone
    time_zone = MAPPING['track.person'][person_type]['timezone']
    # sort time values and convert for output
    close_contact = close_contact.sort_values(['sourceTime','targetTime'])
    close_contact['sourceTime'] = pd.to_datetime(close_contact['sourceTime']).dt.tz_localize('UTC').dt.tz_convert(time_zone)
    close_contact['targetTime'] = pd.to_datetime(close_contact['targetTime']).dt.tz_localize('UTC').dt.tz_convert(time_zone)
    # save result
    close_contact.to_csv(output_folder +'/'+ os.path.basename(file_), index = False)
    
    print(f'File {os.path.basename(file_)}: time Lapsed: {(time()-start_time)/60:.2f}min', flush = True)
    # get the summary of close contact
    close_contact_summary = close_contact.groupby('targetId').apply(summary_close_contact)
    close_contact_summary['sourceId'] = os.path.basename(file_).split('.')[0]
    return close_contact_summary.reset_index()

# helper function of generate agg close contact stats
def agg_close_contact_stats(x):
    name = {'targetTime_min' : x['targetTime'].min(),
            'targetTime_max': x['targetTime'].max(),
            'targetLat_avg':x['targetLat'].mean(),
            'targetLong_avg': x['targetLong'].mean(),
            'contacts': x['targetRefId'].size}
    return pd.Series(name, index=list(name.keys()))

# transform records
def extract_close_contact_stats(file_name, output_folder):
    try:
        df = pd.read_csv(file_name)
    except FileNotFoundError:  # if the file not exists (means no contact record)
        return # early stop
    # get the contact stat
    df = df.groupby(['sourceRefId','sourceId','sourceTime','sourceLat',
                     'sourceLong','targetId']).apply(agg_close_contact_stats)
    # save to new files
    df.reset_index().to_csv(output_folder + '/' + os.path.basename(file_name),index = False)
    
# shorten close contact using statistics
def shorten_close_contact(files, output_folder):
    print(f'Shorten contacts...', end = ' ', flush = True)
    if not os.path.exists(output_folder):
        os.mkdir(output_folder)
    start_time = time()
    with Pool(12) as p:
        func = partial(extract_close_contact_stats, output_folder = output_folder)
        p.map(func, files)
    # output time
    print(f'Complete. Time lapsed: {(time() - start_time)/60 :.2f} min', flush = True)

# write close contact summary
def close_contact_summary(dfs, result_folder, file_name ='close_contact_summary.csv'):
    print('Generating Summary')
    if not os.path.exists(result_folder):
        os.mkdir(result_folder)
    # contact list
    df = pd.concat(dfs, ignore_index = True)
    # save result
    df.to_csv(result_folder + '/' + file_name, index = False)