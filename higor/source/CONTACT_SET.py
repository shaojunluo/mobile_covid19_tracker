import sys
import pickle
import geohash
import numpy as np
import pandas as pd
import itertools as it
import graph_tool.all as gt
from contacts_functions import *
from datetime import datetime
import geopy.distance as gdist
import matplotlib.pyplot as pyplot
from collections import defaultdict, Counter

class CONTACT_SETUP:
    def __init__(self, params_dt_dx=[10, 2]):
        self.list_of_infected = None
        self.unique_infected_id = None
        self.geo_infected_dict = None
        self.infected_contacts = defaultdict(list)
        self.time_window_min = params_dt_dx[0]
        self.time_window_sec = params_dt_dx[0]/60
        self.radius_m = params_dt_dx[1]
        self.hit_model_runned = False
        self.continuous_models_runned = False

    def init_geodict(self, geo_dict_path):
        '''
            For both models, we use a hash table where keys are the length-8
            geohashes of places where infected people passed by during some instant
            during its infeccious period (-15 days before and +15 days after the 
            date of symptons). This useful for efficient search of close infected
            people.
        '''
        with open(geo_dict_path, 'rb') as handle:
            self.geo_infected_dict = pickle.load(handle)

    def read_list_of_infected(self, infected_list_path):
        '''
            Read a csv table containing, as index, the UID of all considered infected
            people.
        '''
        self.list_of_infected = pd.read_csv(infected_list_path)
        self.list_of_infected.sort_values(by=['sympton_date'], inplace=True)
        self.unique_infected_id = list(self.list_of_infected.index)

    def hit_model(self, gps_path, geo_precision=8):
        '''
            Use the so-called 'hit' model to identify contacts between gps infected UIDs
            and healthy ones. For each day, we read the gps data points and check if the
            current user data point is close to an infected UID and if it is inside an time 
            interval where this infected did not move. The minimum size of the stopped time
            interval is given by the 'delta_t' parameter.

            'gps_path': the path for the folder containing the files containing the gps data
                        points for each day.

            'precision':    the number of the first characters of the geohash of the user. This
                            is used to identify which infected UIDs are close to the current user.
                            The precision must be the same of the 'geo_infected_dict' provided.
        '''
        unique_infected = self.unique_infected_id
        time_window_sec = self.time_window_sec
        distance_m = self.radius_m

        # format of daily gps data should be 'dayX.csv' inside a month folder.
        days = ['day{:0>2}.csv'.format(n) for n in range(1,32)] # PAY ATTENTION HERE!
        days_num = [n for n in range(1,32)]

        for day_index, cur_file in enumerate(days):
            print("day {}".format(days_num[day_index]))
            for gpsframe in pd.read_csv(open(gps_path+cur_file, 'r'), encoding='utf-8', engine='c', chunksize=10000):
                for index, row in gpsframe.iterrows():
                    current_uid = row['uid']
                    uid_ghash = row['geohash']
                    timestamp = row['timestamp']

                    # get the geohash neighbors of the current geohash.
                    point_neighbors = geohash.neighbors(uid_ghash[:geo_precision])
                    point_neighbors.append(uid_ghash[:geo_precision])
                    # We check if there was an infected ID around at some point of time.
                    for surroundings in point_neighbors:   
                        if len(self.geo_infected_dict[surroundings])>0: # If some infected ID walked through this place.
                            unique_infected = find_unique(self.geo_infected_dict[surroundings])
                            for infected_uid in unique_infected:
                                if infected_uid==current_uid: continue
                                if is_inside_interval(self.geo_infected_dict[surroundings], infected_uid, uid_ghash, timestamp, time_window_sec, distance_m):
                                    self.infected_contacts[infected_uid].append(current_uid)
                                else:
                                    pass
        self.hit_model_runned = True

    def continuous_model(self, gps_path, geo_precision=8):
        '''
            For the continuous model the goal is to capture the contacts that happened
            during at least 'delta_t' minutes. For this, we get the timeline for both 
            infected and healthy person for the day to check the time overlaps. If the 
            they overlap, check distance.
        '''

        unique_infected = self.unique_infected_id
        time_window_sec = self.time_window_sec
        distance_m = self.radius_m

        # For fast checking between the infected and the
        # not infected people.
        infected_id_dict = defaultdict(lambda:-1)
        for cur_id in self.unique_infected_id:
            infected_id_dict[cur_id] = 1

        # For whole month.
        days = ['day{:0>2}.csv'.format(n) for n in range(1,32)] # PAY ATTENTION HERE!
        days_num = [n for n in range(1,32)]

        geo_precision = 8
        # Loop through the days.
        for day_index, cur_file in enumerate(days):
            print("day {}".format(days_num[day_index]))

            gpsframe = pd.read_csv(open(gps_path+cur_file, 'r'), dtype={'uid':str, 'timestamp':'uint64', 'geohash':str},  encoding='utf-8', engine='c')
            # Group the table by the IDs.
            gpsframe = gpsframe.groupby('uid') 
            # Get all the unique IDs.
            all_daily_uids = list(gpsframe.groups.keys())
            daily_uid = defaultdict(lambda: -1)
            for uid in all_daily_uids: daily_uid[uid] = 1

            print('number of different ids for the day is {}'.format(len(all_daily_uids)))
            for current_uid in all_daily_uids:
                if infected_id_dict[current_uid]==-1: continue
                current_uid_table = gpsframe.get_group(current_uid)
                current_uid_table = current_uid_table.sort_values(by=['timestamp'], ascending=True)
                person_geo = current_uid_table['geohash']
                person_timeline = current_uid_table['timestamp']

                # Get all the unique geohash(8) and its neighbors.
                unique_geo = get_unique_geohashes(person_geo)
                unique_geo = add_neighbors(unique_geo)
                # Get the unique infected IDs.
                unique_inf = find_unique_infected(unique_geo, self.geo_infected_dict)

                # We get the timeline(if there is one in the current day) of all infected IDs to compare
                # with the current healthy person.
                for infected_uid in unique_inf:
                    if infected_id_dict[infected_uid]==-1: continue
                    if infected_uid==current_uid: continue
                    if daily_uid[infected_uid]==1:
                        infected_table = gpsframe.get_group(infected_uid)
                        infected_table = infected_table.sort_values(by=['timestamp'], ascending=True)
                        infected_geo = infected_table['geohash']
                        infected_timeline = infected_table['timestamp']

                    # We do not need to check contacts if their timeline does not overlap.
                    if (person_timeline.iloc[-1]<infected_timeline.iloc[0]) or (person_timeline.iloc[0]>infected_timeline.iloc[-1]): 
                        continue
                    else:
                        overlap_time = have_contact(person_geo, person_timeline, infected_geo, infected_timeline, time_window_sec, distance_m)
                        if overlap_time>0:
                            infected_contacts[infected_uid].append((current_uid, overlap_time))
        self.continuous_model_runned = True

    def format_number_contacts_hit(self):
        if self.hit_model_runned==False:
            print('must generate the contact dict first.')
        
        for key in self.infected_contacts.keys():
            # capture repeated hit contacts.
            self.infected_contacts[key] = Counter(self.infected_contacts[key])

    def get_contacts_dict(self):
        if self.hit_model_runned==False:
            return None
        else:
            return self.infected_contacts

    def store_object_as(self, output_pathname):
        with open(output_pathname, 'wb') as handle:
            pickle.dump(self.infected_contacts)

