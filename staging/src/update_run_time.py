
import yaml
import pandas as pd
import os

# update the runing time after all pipeline is finished.
with open(os.path.dirname(__file__) + '/../config_status.yaml','r') as f:
    file_status = yaml.safe_load(f)
    # update the running time
    file_status['last_run'] = str(pd.Timestamp.now(tz = 'US/Eastern'))

# save the running status
with open(os.path.dirname(__file__) + '/../config_status.yaml','w') as f:
    yaml.safe_dump(file_status, f)