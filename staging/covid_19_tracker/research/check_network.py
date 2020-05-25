import pandas as pd
import numpy as np
import networkx as nx
from matplotlib import pyplot as plt

l_max = 19
wkdir = 'deliver/calibration_25'
edge_list = pd.read_csv(wkdir + '/final_edges.csv', parse_dates= ['sourceTime'])
chunks = [pd.read_csv(f'{wkdir}/active_chunk_{i}.csv', parse_dates= ['sourceTime']) for i in range(l_max)]

# Get Graph
G = nx.from_pandas_edgelist(edge_list, 'sourceId', 'targetId', edge_attr=True)
# level 0
ps0 = set(pd.unique(chunks[0].loc[chunks[0]['has_track'],'targetId']))
ps = ps0.copy()
# initiatize
n_G =[]
nk = []
n1 = []
deg = []
for lvl in range(l_max):
    ps_tmp = set(pd.unique(chunks[lvl]['targetId']))
    ps.update(ps_tmp)
    # append graph
    n_G.append(len(ps))
    #sub graph
    G_sub = G.subgraph(ps)
    deg.append(np.mean(list(dict(G_sub.degree()).values())))
    # find fist layer
    neighbors = ps0.copy()
    for n in ps0:
        if n in G_sub:
            nb = set(nx.all_neighbors(G_sub, n))
            neighbors.update(nb)
    n1.append(len(neighbors.difference(ps0)))
    nk.append(len(ps_tmp))
    n0 = len(ps0)
    print(f'Subgraph Chunk {lvl}: new nodes added {nk[-1]}')
    print(f'number of 1st layer: {n1[-1]}, number of 0 layer: {n0}. R_note: {n1[-1]/n0:.3f}')

fig = plt.figure()
ax = fig.subplots(2,2)
ax[0,0].plot(n_G,'+-')
ax[0,0].set_xlabel('Chunk Added')
ax[0,0].set_ylabel('Network Size')

ax[0,1].plot(deg,'+-')
ax[0,1].set_xlabel('Chunk Added')
ax[0,1].set_ylabel('Average Degree')

ax[1,0].plot(nk,'+-')
ax[1,0].set_xlabel('Chunk')
ax[1,0].set_ylabel('Chunk Size')

ax[1,1].plot(range(1,len(n1)-1), np.array(n1[1:-1])/n0,'+-')
ax[1,1].set_xlabel('Chunk Added')
ax[1,1].set_ylabel('$R_0$')

fig.tight_layout()
fig.show()

