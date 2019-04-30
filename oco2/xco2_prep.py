"""
For parallel jobs: use `qsub jobscript` on sciclone

https://co2.jpl.nasa.gov/#mission=OCO-2
https://docserver.gesdisc.eosdis.nasa.gov/public/project/OCO/OCO2_DUG.V9.pdf

"""
import os
import errno
import glob
import copy
import h5py
import rasterio
import pandas as pd
import numpy as np
from scipy.interpolate import griddata
from affine import Affine

# mode = "serial"
# mode = "parallel"
mode = "auto"

# resolution of grid points and interpolated raster output
rnd_interval = 0.1
decimal_places = len(str(rnd_interval).split(".")[1])

# methods for aggregating raw data to regular grid
agg_ops = {
    'xco2': "mean",
    'xco2_quality_flag': "count",
    'longitude': "first",
    'latitude': 'first'
}

# interpolation method
interp_method = "linear"


run_a = False
run_b = True
run_c = True
run_d = True
run_e = True
run_f = True
run_g = True

raw_dir = "/sciclone/aiddata10/REU/geo/raw/jpl/oco2"

data_dir = "/sciclone/aiddata10/REU/geo/data/rasters/jpl/xco2"

day_dir = os.path.join(data_dir, "day")
month_dir = os.path.join(data_dir, "month")
month_grid_dir = os.path.join(data_dir, "month_grid")
month_interp_dir = os.path.join(data_dir, "month_interp")
year_dir = os.path.join(data_dir, "year")
year_grid_dir = os.path.join(data_dir, "year_grid")
year_interp_dir = os.path.join(data_dir, "year_interp")


# -----------------------------------------------------------------------------


def convert_daily(f):
    """convert daily nc4 files to csv
    """
    id_string = os.path.basename(f).split("_")[2]
    print "Converting {}".format(id_string)
    with h5py.File(f, 'r') as hdf_data:
        xco2 = copy.deepcopy(list(hdf_data["xco2"]))
        lon = copy.deepcopy(list(hdf_data["longitude"]))
        lat = copy.deepcopy(list(hdf_data["latitude"]))
        xco2_quality_flag = copy.deepcopy(list(hdf_data["xco2_quality_flag"]))
    point_list = []
    for i in range(len(xco2)):
        point_list.append({
            "longitude": lon[i],
            "latitude": lat[i],
            "xco2": xco2[i],
            "xco2_quality_flag": xco2_quality_flag[i]
        })
    df = pd.DataFrame(point_list)
    df_path = os.path.join(day_dir, "xco2_{}.csv".format(id_string))
    df.to_csv(df_path, index=False, encoding='utf-8')


def concat_data(id_string, flist, out_path):
    """concat daily data csv to monthly data csv
    """
    df_list = [read_csv(f) for f in flist]
    out = pd.concat(df_list, axis=0, ignore_index=True)
    out.to_csv(out_path, index=False, encoding='utf-8')


def concat_month(task):
    id_string, flist = task
    print "Concat yearmonth {}".format(id_string)
    out_path = os.path.join(month_dir, "xco2_{}.csv".format(id_string))
    concat_data(id_string, flist, out_path)


def concat_year(task):
    id_string, flist = task
    print "Concat year {}".format(id_string)
    out_path = os.path.join(year_dir, "xco2_{}.csv".format(id_string))
    concat_data(id_string, flist, out_path)


def round_to(value, interval):
    """round value to nearest interval of a decimal value
    e.g., every 0.25
    """
    if interval > 1:
        raise ValueError("Must provide float less than (or equal to) 1 indicating interval to round to")
    return round(value * (1/interval)) * interval


def lonlat(lon, lat, dlen):
    """create unique id string combining latitude and longitude
    """
    str_lon = format(lon, '0.{}f'.format(dlen))
    str_lat = format(lat, '0.{}f'.format(dlen))
    lon_lat = "{}_{}".format(str_lon, str_lat)
    return lon_lat


def agg_to_grid(f, agg_path):
    """aggregate coordinates to regular grid points
    """
    df = read_csv(f)
    df = df.loc[df["xco2_quality_flag"] == 0].copy(deep=True)
    df["longitude"] = df["longitude"].apply(lambda z: round_to(z, rnd_interval))
    df["latitude"] = df["latitude"].apply(lambda z: round_to(z, rnd_interval))
    df["lonlat"] = df.apply(lambda z: lonlat(z["longitude"], z["latitude"], decimal_places), axis=1)
    agg_df = df.groupby('lonlat', as_index=False).agg(agg_ops)
    agg_df.columns = [i.replace("xco2_quality_flag", "count") for i in agg_df.columns]
    agg_df.to_csv(agg_path, index=False, encoding='utf-8')


def agg_to_grid_month(f):
    id_string = os.path.basename(f).split("_")[1][0:4]
    print "Agg {}".format(id_string)
    agg_path = os.path.join(month_grid_dir, "xco2_{}.csv".format(id_string))
    agg_to_grid(f, agg_path)


def agg_to_grid_year(f):
    id_string = os.path.basename(f).split("_")[1][0:2]
    print "Agg {}".format(id_string)
    agg_path = os.path.join(year_grid_dir, "xco2_{}.csv".format(id_string))
    agg_to_grid(f, agg_path)


def interpolate(f, raster_path):
    """interpolate
    https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.griddata.html#scipy.interpolate.griddata
    https://earthscience.stackexchange.com/questions/12057/how-to-interpolate-scattered-data-to-a-regular-grid-in-python
    """
    data = read_csv(f)
    # data coordinates and values
    x = data["longitude"]
    y = data["latitude"]
    z = data["xco2"]
    # target grid to interpolate to
    xi = np.arange(-180.0, 180.0+rnd_interval, rnd_interval)
    yi = np.arange(90.0, -90.0-rnd_interval, -rnd_interval)
    xi, yi = np.meshgrid(xi,yi)
    # interpolate
    zi = griddata((x, y), z, (xi, yi), method=interp_method)
    # prepare raster
    affine = Affine(rnd_interval, 0, -180.0,
                    0, -rnd_interval, 90.0)
    meta = {
        'count': 1,
        'crs': {'init': 'epsg:4326'},
        'dtype': str(zi.dtype),
        'affine': affine,
        'driver': 'GTiff',
        'height': zi.shape[0],
        'width': zi.shape[1]
    }
    raster_out = np.array([zi])
    with rasterio.open(raster_path, "w", **meta) as dst:
        dst.write(raster_out)


def interpolate_month(f):
    id_string = os.path.basename(f).split("_")[1][0:4]
    print "Interpolating {}".format(id_string)
    raster_path = os.path.join(month_interp_dir, "xco2_20{}_{}.tif".format(id_string, interp_method))
    interpolate(f, raster_path)


def interpolate_year(f):
    id_string = os.path.basename(f).split("_")[1][0:2]
    print "Interpolating {}".format(id_string)
    raster_path = os.path.join(year_interp_dir, "xco2_20{}_{}.tif".format(id_string, interp_method))
    interpolate(f, raster_path)


def make_dir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def read_csv(path):
    df = pd.read_csv(
        path, quotechar='\"',
        na_values='', keep_default_na=False,
        encoding='utf-8')
    return df


def run(tasks, func):
    parallel = False
    if mode in ["auto", "parallel"]:
        try:
            from mpi4py import MPI
            parallel = True
        except:
            parallel = False
    elif mode != "serial":
        raise Exception("Invalid `mode` value for script.")
    if parallel:
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()
    else:
        size = 1
        rank = 0
    c = rank
    while c < len(tasks):
        try:
            func(tasks[c])
        except Exception as e:
            print "Error processing: {0}".format(tasks[c])
            # raise
            print e
        c += size
    if parallel:
        comm.Barrier()


# -----------------------------------------------------------------------------
# prepare daily data

make_dir(day_dir)

search_regex = os.path.join(raw_dir, "oco2_LtCO2_*.nc4")
qlist_a = glob.glob(search_regex)

if run_a:
    run(qlist_a, convert_daily)


# -----------------------------------------------------------------------------
# concat all daily for each month

make_dir(month_dir)

dlist = glob.glob(os.path.join(day_dir, 'xco2_*.csv'))

qlist_dict = {}
for i in dlist:
    yearmonth = os.path.basename(i).split("_")[1][0:4]
    if yearmonth not in qlist_dict:
        qlist_dict[yearmonth] = []
    qlist_dict[yearmonth].append(i)

qlist_b = qlist_dict.items()

if run_b:
    run(qlist_b, concat_month)


# -----------------------------------------------------------------------------
# concat all month for each year

make_dir(year_dir)

mlist = glob.glob(os.path.join(month_dir, 'xco2_*.csv'))

qlist_dict = {}
for i in mlist:
    year = os.path.basename(i).split("_")[1][0:2]
    if year not in qlist_dict:
        qlist_dict[year] = []
    qlist_dict[year].append(i)

qlist_c = qlist_dict.items()

if run_c:
    run(qlist_c, concat_year)


# -----------------------------------------------------------------------------
# agg monthly data to grid

make_dir(month_grid_dir)

# id_list = ["1501"]
# qlist_d = [os.path.join(month_dir, "xco2_{}.csv".format(id_string)) for id_string in id_list]

qlist_d = glob.glob(os.path.join(month_dir, 'xco2_*.csv'))

if run_d:
    run(qlist_d, agg_to_grid_month)


# -----------------------------------------------------------------------------
# interpolate month grid data to fill gaps

make_dir(month_interp_dir)

qlist_e = glob.glob(os.path.join(month_grid_dir, 'xco2_*.csv'))

if run_e:
    run(qlist_e, interpolate_month)


# -----------------------------------------------------------------------------
# agg yearly data to grid

make_dir(year_grid_dir)

# id_list = ["1501"]
# qlist_d = [os.path.join(month_dir, "xco2_{}.csv".format(id_string)) for id_string in id_list]

qlist_f = glob.glob(os.path.join(year_dir, 'xco2_*.csv'))

if run_f:
    run(qlist_f, agg_to_grid_year)


# -----------------------------------------------------------------------------
# interpolate year grid data to fill gaps

make_dir(year_interp_dir)

qlist_g = glob.glob(os.path.join(year_grid_dir, 'xco2_*.csv'))

if run_g:
    run(qlist_g, interpolate_year)

