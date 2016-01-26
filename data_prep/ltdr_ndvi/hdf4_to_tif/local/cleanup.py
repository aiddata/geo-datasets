# some data was processed one sensor at a time so there may be year overlaps
# need to clean that up and keep data from most recent sensor where year/day duplicates exist


import sys
import os
import errno

# base path where data is located
path_base = "/home/userz/globus-data/data/ltdr.nascom.nasa.gov/allData/Ver4"


# reference object used to eliminate duplicate year / day combos
# when overlaps between sensors exists, always use data from newer sensor
ref = {}

# list of all [year, day] combos


# get sensors
sensors = [name for name in os.listdir(path_base) if os.path.isdir(os.path.join(path_base, name))]
sensors.sort()

# use limited sensors for testing 
# sensors = ['2001']

for sensor in sensors:

	# get years for sensors
	path_sensor = path_base +"/"+ sensor
	years = [name for name in os.listdir(path_sensor) if os.path.isdir(os.path.join(path_sensor, name))]
	years.sort()


	for year in years:

		if not year in ref:
			ref[year] = {}


		# get days for year
		path_year = path_sensor +"/"+ year
		filenames = [name for name in os.listdir(path_year) if not os.path.isdir(os.path.join(path_year, name)) and name.endswith(".tif") and name.split(".")[0] == "AVH13C1"]
		filenames.sort()

		for filename in filenames:

			filename = filename[:-4]
			day = filename.split(".")[1][5:]

			if not day in ref[year] or int(sensor[1:]) > int(ref[year][day][0][1:]):
				ref[year][day] = [sensor, filename]


# creates directories 
def make_dir(path):
	try: 
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise


qlist = []
for year in ref:

	for day in ref[year]:

 		# qlist.append(ref[year][day] + [year, day]) 
 		pin = "/home/userz/globus-data/data/ltdr.nascom.nasa.gov/allData/Ver4/"+ref[year][day][0]+"/"+year+"/"+ref[year][day][1]+".tif"

 		pout_dir = "/home/userz/globus-data/data/ltdr.nascom.nasa.gov/allData/ndvi/"+year
 		pout = pout_dir+"/"+ref[year][day][1]+".tif"

 		make_dir(pout_dir)

 		os.rename(pin, pout)
 		# print pout
 		# copy file
 		# 