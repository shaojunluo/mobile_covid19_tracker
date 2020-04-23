import os
import sys
from functools import partial
from glob import glob
from multiprocessing import Pool
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

## ==================== Select subset of patient from close contact list ===================

# join close contact table one by one
def filter_close_contact(file_, patient_list, id_col):
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
    func = partial(filter_close_contact, patient_list = patient_list, id_col = id_col)
    # usin parallel for processing
    with Pool(n_workers) as p:
        dfs = list(p.map(func, query_files))
    df = pd.concat(dfs).sort_values(['sourceId','sourceTime','targetTime'])
        
    return df.drop_duplicates()