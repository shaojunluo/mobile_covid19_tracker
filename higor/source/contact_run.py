import sys
from CONTACT_SET import CONTACT_SETUP

# 'gps_path' is the path for the folder containing the daily gps data - 'dayX.csv'.
gps_path = "../Data/gps/March/"     
list_of_infected_path = "../Data/list_of_infected.csv"
geo_dict_path = "../Data/infected_places.pickle"
output_pathname = "../Data/test/name_of_output.pickle"

#deltat = float(sys.argv[1]) # In minutes.
#deltax = float(sys.argv[2]) # In meters.
deltat = 10
deltax = 2

params_dt_dx = [deltat, deltax]

# create model object.
model = CONTACT_SETUP(params_dt_dx=params_dt_dx)

# Initialize the model with the dictionary of infected geohashes and
# the table containing the list of infected.
model.init_geodict(geo_dict_path)
model.read_list_of_infected(list_of_infected_path)

# Then, run the model
model.hit_model(gps_path)

# Format the dictionary of contacts.
model.format_number_contacts_hit()

''' 
    The output is a hash table where each key is the ID of an infected
    people and it returns a list of tuples (ID, number_of_hits) of its 
    unique contacts.
''' 
contacts = model.get_contacts_dict()

model.store_object_as(output_pathname)
