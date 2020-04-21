import pandas as pd
import numpy as np
from time import time
from glob import glob
import os
import utiles
from elasticsearch import Elasticsearch
from matplotlib import pyplot as plt
from importlib import reload
reload(utiles)

### The CODE is not being used right now but leave for future modeling ####

df = pd.read_csv('patient_track/3E5441C6BA4B342E27BC97CF4106E8CE260519E2.csv')
df['acquisitionTime'] = pd.to_datetime(df['acquisitionTime'])

fig = plt.figure()
ax = fig.subplots()

ax.scatter(df['lon'], df['lat'], s = 5, c = df['movingRate'] > 1)
ax.set_xlim([-38.5, -38.46])
ax.set_ylim([-3.74, -3.72])
fig.show()
