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
    input_folder = params['ingestion']['input.folder']
    mode = params['ingestion']['mode']

files = glob(input_folder + '/*.csv') # fetch files

### ======================== Sequencial Processing ========================
### Best Run in local machine 

index_list = [utils.read_to_elastic(file_, host_url = HOST_URL, 
                                        port = PORT, 
                                        n_thread = n_thread,
                                        mode = mode,
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
utils.add_ingested_index(index_list)