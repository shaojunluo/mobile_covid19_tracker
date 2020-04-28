import yaml
from glob import glob
import os
import sys

# add the directory
sys.path.append(os.path.dirname(__file__) + '/..')
# read the relavant libaries
import lib.lib_ingestion as utils

# load configuration file and fetch files
with open(os.path.dirname(__file__) + '/../config_params.yaml','r') as f:
    params = yaml.safe_load(f)
    HOST_URL = params['elasticsearch']['host']
    PORT = params['elasticsearch']['port']
    n_thread = params['elasticsearch']['thread']
    input_app = params['ingestion']['input.folder']['app']
    input_grandata = params['ingestion']['input.folder']['grandata']

### ======================== Sequencial Processing ========================
### Best Run in local machine 
print('ingesting App data')
# ingest input of app first
files = glob(input_app + '/*.csv') # fetch files
# Ingest input of app
index_list = [utils.read_to_elastic(file_, 'app',
                                    host_url = HOST_URL, 
                                    port = PORT, 
                                    n_thread = n_thread,
                                    # default_mode: action to take if the index already exists. 
                                    # valid value: skip, append, overwrite
                                    mode = utils.ingestion_mode('app', default_mode = 'overwrite'), 
                                    prefix = 'fortaleza_') for file_ in files]

print('ingesting Pre-exist patient data')
files = glob(input_grandata + '/*.csv') # fetch files
# Ingest input of grandata
index_list = [utils.read_to_elastic(file_, 'grandata',
                                    host_url = HOST_URL, 
                                    port = PORT, 
                                    n_thread = n_thread,
                                    mode = utils.ingestion_mode('app', default_mode = 'append'),
                                    prefix = 'fortaleza_') for file_ in files]
    
### ======================== Parrellel Processing =========================
### use this if you have large machine and querying other server

# # Put the worker to parallel
# func = partial(utils.read_to_elastic, data_source ='app',
#                                       host_url = HOST_URL, 
#                                       port = PORT, 
#                                       n_thread = n_thread,
#                                       mode = utils.ingestion_mode('app', default_mode = 'skip'),
#                                       prefix = 'fortaleza_')
# # ingest app data
# print('ingesting App data')
# with Pool(6) as p: # use smaller size to avoid memory overflow
#     files = glob(input_app + '/*.csv')
#     index_list = list(p.map(func, files))
    
# # Put the worker to parallel
# print('ingesting Pre-exist patient data')
# func = partial(utils.read_to_elastic, data_source ='grandata',
#                                       host_url = HOST_URL, 
#                                       port = PORT, 
#                                       n_thread = n_thread,
#                                       mode = utils.ingestion_mode('app', default_mode = 'append'),
#                                       prefix = 'fortaleza_')
# # input with grandata
# with Pool(12) as p:
#     files = glob(input_grandata + '/*.csv')
#     index_list = list(p.map(func, files))

### update ingested list
utils.add_ingested_index(index_list)