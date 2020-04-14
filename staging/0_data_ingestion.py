import yaml
from glob import glob
import lib_utils

# load configuration file and fetch files
with open('config_params.yaml', 'r') as stream:
    params = yaml.safe_load(stream)
    HOST_URL = params['elasticsearch']['host']
    PORT = params['elasticsearch']['port']
    n_thread = params['elasticsearch']['thread']
    input_folder = params['ingestion']['input.folder']

files = glob(input_folder + '/*.csv') # fetch files

### ======================== Sequencial Processing ========================
### Best Run in local machine 

index_list = [lib_utils.read_to_elastic(file_, host_url = HOST_URL, 
                                               port = PORT, 
                                               n_thread = n_thread,
                                               prefix = 'fortaleza_') for file_ in files]
    
### ======================== Parrellel Processing =========================
### use this if you have large machine and querying other server

# # library for parallel computing
# from functools import partial
# from multiprocessing import Pool

# # Put the worker to parallel
# func = partial(utiles.read_to_elastic, host_url = HOST_URL, port = PORT, n_thread = 6,prefix = 'fortaleza_')
# with Pool(4) as p:
#     index_list = list(p.map(func, files))

### update ingested list
lib_utils.add_ingested_index(index_list)
