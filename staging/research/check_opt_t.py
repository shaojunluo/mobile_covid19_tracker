import pandas as pd
import numpy as np
import networkx as nx
from matplotlib import pyplot as plt

# cl = {}
# cl_r = {}
# hists = []
# hists_r = []
# x = list(range(10,61,5))
# for t in x:
#     cl[t] = pd.read_csv(f'deliver/opt_t/t_{t}/risky_contacts.csv')['p']
#     cl_r[t] = pd.read_csv(f'deliver/opt_t/edges_{t}.csv')['p']
#     h, _ = np.histogram(cl[t],bins = np.linspace(0,1,11))
#     h_r,_ = np.histogram(cl_r[t],bins = np.linspace(0,1,11))
#     hists.append(h/h.sum())
#     hists_r.append(h_r/h_r.sum())
    
# h = np.stack(hists).T
# h_r = np.stack(hists_r).T

# fig= plt.figure(figsize = (10,5))
# ax = fig.subplots(1,2)
# ax[0].imshow(h, origin = 'lower',vmin = 0, vmax = 0.3)
# ax[1].imshow(h_r, origin = 'lower',vmin = 0)

# p_median = [10*cl[t].quantile(0.5) for t in x] 
# p_mean = [10*cl[t].mean() for t in x] 
# ax[0].plot(p_median,label = 'median')
# ax[0].plot(p_mean, label = 'mean')
# ax[0].set_xticks(range(len(x)))
# ax[0].set_xticklabels(x)
# ax[0].set_yticks(np.linspace(0,10,11)-0.5)
# ax[0].set_yticklabels(np.round(np.linspace(0,1,11),1))
# ax[0].set_title('Distribution of P')
# ax[0].set_xlabel('$\Delta T (min)$')
# ax[0].set_ylabel('$p$')
# ax[0].legend()

# p_median = [10*cl_r[t].quantile(0.5) for t in x] 
# p_mean = [10*cl_r[t].mean() for t in x] 
# ax[1].plot(p_median,label = 'median')
# ax[1].plot(p_mean, label = 'mean')
# ax[1].set_xticks(range(len(x)))
# ax[1].set_xticklabels(x)
# ax[1].set_yticks(np.linspace(0,10,11)-0.5)
# ax[1].set_yticklabels(np.round(np.linspace(0,1,11),1))
# ax[1].set_title('Distribution of Recursive P')
# ax[1].set_xlabel('$\Delta T (min)$')
# ax[1].set_ylabel('$p$')
# ax[1].legend()


# fig.tight_layout()

# fig.show()

fig2 = plt.figure()
ax = fig2.subplots(1)
p_25 = pd.read_csv(f'deliver/opt_t/edges_25.csv')['p']
p2_25 = pd.read_csv(f'deliver/opt_t/t_25/risky_contacts.csv')['p']
ax.hist(p_25, bins = np.linspace(0,1,101),alpha = 0.7, label = 'recursive_p',density = True)
ax.hist(p2_25, bins = np.linspace(0,1,101),alpha = 0.7, label = 'p',density = True)

ax.legend()
fig2.show()