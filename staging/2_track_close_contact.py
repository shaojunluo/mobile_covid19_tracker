import yaml
from time import time

from glob import glob
from functools import partial
from multiprocessing import Pool

import lib_utils

person_type = 'patient.higor'

# read parameter of the step
with open('config_params.yaml', 'r') as stream:
    params = yaml.safe_load(stream)
    HOST_URL = params['elasticsearch']['host']
    PORT = params['elasticsearch']['port']
    input_folder =  params['track.close_contact'][person_type]['input.folder']
    output_folder = params['track.close_contact'][person_type]['output.folder'] 
    m_before = params['track.close_contact'][person_type]['minute.before']
    m_after =  params['track.close_contact'][person_type]['minute.after']
    min_d =    params['track.close_contact'][person_type]['distance']

# read files
files = glob(input_folder + '/*.csv')
# Track the list of close contact
func = partial(lib_utils.track_close_contact, output_folder = output_folder, 
               minutes_before = m_before, minutes_after = m_after, distance = min_d,
               host_url = HOST_URL, port = PORT, index_prefix = 'fortaleza_')

# Parrelle processing
with Pool(12) as p:
    p.map(func, files)