import sys
import pickle
import geohash
import numpy as np
import pandas as pd
import itertools as it
import graph_tool.all as gt
from datetime import datetime
import geopy.distance as gdist
import matplotlib.pyplot as pyplot
from collections import defaultdict

def find_unique(infected_list):
    uids = [ info[0] for info in infected_list]
    return list(set(uids))

def interval_dist_bygeohash(geo_points, time_points, precision=11):
    '''
        Defines the delta_x and delta_t arrays using only the similarity of
        the geohashes. We can not calculate the actual distance with this approach,
        but we can identify the intervals where people stay at the same place. Faster.
    '''
    dist_list = []
    dt_list = np.array([time_points[i+1]-time_points[i] for i in range(0, len(geo_points)-1)])
    for i in range(0,len(geo_points)-1):
        #lat0, long0 = geohash.decode(geo_points[i])
        #lat1, long1 = geohash.decode(geo_points[i+1])
        if geo_points[i][:precision]==geo_points[i+1][:precision]:
            d = 0.0
        else: d = 10.0
        #if d<0.10: d = 0.00
        dist_list.append(d)
    return (np.array(dist_list), dt_list)

def interval_dist_bydist(geo_points, time_points, precision=11):
    '''
        Defines the delta_x and delta_t arrays.
    '''
    dist_list = []
    dt_list = np.array([time_points[i+1]-time_points[i] for i in range(0, len(geo_points)-1)])
    for i in range(0,len(geo_points)-1):
        lat0, long0 = geohash.decode(geo_points[i])
        lat1, long1 = geohash.decode(geo_points[i+1])
        #if geo_points[i][:precision]==geo_points[i+1][:precision]:
            #d = 0.0
        d = gdist.geodesic((lat0, long0), (lat1, long1)).m
        #else: d = 10.0
        if d<0.10: d = 0.00
        dist_list.append(d)
    return (np.array(dist_list), dt_list)

def define_index_size_sequence(dx_person, threshold=1e-5):
    i = 0
    all_zero_interval = []
    for key, group in it.groupby(dx_person):
        elems = len(list(group))
        if key<1e-5:
            all_zero_interval.append((i, elems))   # start index, and the size of the sequence.
        i += elems
    return all_zero_interval

###### For the hit model #########
def is_inside_interval(list_infected, infected_uid, current_geohash, current_id_time, time_window_sec, distance_m, precision=8):
    '''
        Gets all the timestamps of 'infected_uid' inside 'infected_geohash'[:'precision'].
        'list_infected' is the list containing all the infected data points that passed by
        'infected_geohash'[:'precision'].
    '''
    IDS = [ info[0] for info in list_infected if info[0]==infected_uid ]
    GEO = [ info[1] for info in list_infected if info[0]==infected_uid ]
    TIME = [ info[2] for info in list_infected if info[0]==infected_uid ]
    
    # Sort the geohashes points according their timestamps.
    infected_loc = [x for _,x in sorted(zip(TIME,GEO))]
    infected_time = sorted(TIME)

    # define the delta_t and delta_x arrays.
    dx_infected, dt_infected = interval_dist_bygeohash(infected_loc, infected_time)
    # get the index of only the intervals with no displacements.
    all_zero_interval_infected = define_index_size_sequence(dx_infected)

    for inf_start_index, inf_seq_size in all_zero_interval_infected:
        inf_time_points = infected_time[inf_start_index:inf_start_index+inf_seq_size+1]
        geo_inf = infected_loc[inf_start_index]

        interval_size = inf_time_points[-1]-inf_time_points[0]

        if interval_size>=time_window_sec:
            # Is inside the interval of the infected?
            if current_id_time >= inf_time_points[0] and current_id_time <= inf_time_points[-1]:
                lat_h, long_h = geohash.decode(current_geohash)
                lat_inf, long_inf = geohash.decode(geo_inf)
                d = gdist.geodesic((lat_h, long_h), (lat_inf, long_inf)).m
                if d<distance_m:
                    print('{} < {} < {}'.format(inf_time_points[0], current_id_time, inf_time_points[-1]))
                    return True
                else:
                    return False
            else:
                return False
        return False

######################################################################################################
######################################################################################################
def get_unique_geohashes(geo_list, precision=8):
    unique_geo = []
    for index, element in geo_list.items():
        unique_geo.append(element[:precision])
    return list(set(unique_geo))

def add_neighbors(unique_geo):
    lstlst =  [ geohash.neighbors(g) for g in unique_geo ]
    for geo in lstlst:
        unique_geo += geo
    return list(set(unique_geo))

def find_unique_infected(unique_geo, geo_dict):
    unique_infected = []
    info_lstlst = [ geo_dict[cur_geo] for cur_geo in unique_geo]
    
    # 'info' is a list of tuples (uid, geohash(12), timestamp)
    for info in info_lstlst:    
        temp = [ uid[0] for uid in info ]
        unique_infected += temp
    
    return list(set(unique_infected))


##### For the continuous model ######
def have_contact(person_geo, person_timeline, infected_geo, infected_timeline, time_window_sec, distance_m, precision=8):
    '''
        Get the timeline for both infected and healthy person for the day to check the time
        overlaps. If the they overlap, check distance.
    '''
    t0_person = person_timeline.iloc[0]
    t0_infected = infected_timeline.iloc[0]
    tf_person = person_timeline.iloc[-1]
    tf_infected = infected_timeline.iloc[-1]

    person_timeline.reset_index(inplace=True, drop=True)
    person_geo.reset_index(inplace=True, drop=True)
    infected_timeline.reset_index(inplace=True, drop=True)
    infected_geo.reset_index(inplace=True, drop=True)

    start = max([t0_person, t0_infected])
    stop = min([tf_person, tf_infected])

    #### Get the only the points that are common to both. ####
    person_time = person_timeline[(person_timeline>=start) & (person_timeline<=stop)].tolist()
    person_loc = person_geo[(person_timeline>=start) & (person_timeline<=stop)].tolist()

    infected_time = infected_timeline[(infected_timeline>=start) & (infected_timeline<=stop)].tolist()
    infected_loc = infected_geo[(infected_timeline>=start) & (infected_timeline<=stop)].tolist()

    # define the delta_t and delta_x arrays.
    dx_person, dt_person = interval_dist_bydist(person_loc, person_time)
    dx_infected, dt_infected = interval_dist_bydist(infected_loc, infected_time)

    # Now get the start index and the size of the time sequences where the infected was in the same place.
    # Then, we do the same for the healthy person. Then we check each sequence of the healthy with all the
    # stopped sequences of the infected.
    all_zero_interval_infected = define_index_size_sequence(dx_infected)
    all_zero_interval_healthy = define_index_size_sequence(dx_person)
    # Calculate the amount of the overlap time.
    for h_start_index, h_seq_size in all_zero_interval_healthy:
        # get a stopped sequence of the healthy person.
        h_time_points = person_time[h_start_index:h_start_index+h_seq_size+1]
        # Since all the points are in the same place, the we use the first index.
        geo_h = person_loc[h_start_index]   
        lat_h, long_h = geohash.decode(geo_h)
        for inf_start_index, inf_seq_size in all_zero_interval_infected:
            # compare with all the stopped sequences of the infected guy.
            inf_time_points = infected_time[inf_start_index:inf_start_index+inf_seq_size+1]
            geo_inf = infected_loc[inf_start_index]
    
            if h_time_points[-1]<inf_time_points[0] or inf_time_points[-1]<h_time_points[0]:
                return -1
            else:
                # time overlap calculation.
                t_0_interval = max([h_time_points[0], inf_time_points[0]])
                t_f_interval = min([h_time_points[-1], inf_time_points[-1]])
                time_overlap = (t_f_interval - t_0_interval)
                if time_overlap>=time_window_sec:
                    lat_inf, long_inf = geohash.decode(geo_inf)
                    # Check if the two persons are close to the other.
                    dist = gdist.geodesic((lat_h, long_h), (lat_inf, long_inf)).m
                    if dist<distance_m:
                        print('time overlap {}'.format(time_overlap))
                        return time_overlap
                    else:
                        return -1
    return -1