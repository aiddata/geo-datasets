"""
filters "raw_monthly" vcmcfg tiles based on minimum cloud free scenes
threshold to produce "filter_monthly" along with "cloud count" tiles
aggregates and mosaic. cloud count layer pixels represent sum of pixels
across all process tiles where cloud free observation were below specified
threshold
"""


import os
import errno
import rasterio
from rasterio.merge import merge as tile_mosaic
import numpy as np


# -----------------------------------------------------------------------------


# tile_id_list = ["00N060E", "00N060W", "00N180W", "75N060E", "75N060W", "75N180W"]

data_path = "/sciclone/aiddata10/REU/geo/raw/viirs/vcmcfg_dnb_composites_v10/raw_monthly"
out_path = "/sciclone/aiddata10/REU/geo/raw/viirs/vcmcfg_dnb_composites_v10/filtered_monthly"
cloud_count_path = "/sciclone/aiddata10/REU/geo/raw/viirs/vcmcfg_dnb_composites_v10/cloud_count"

# minimum cloud free day threshold
cf_minimum = 2

years = [2017, 2018]

# -----------------------------------------------------------------------------

"""
dict where tile ids are keys and values are list of file_tuples
e.g., {"00N060E": [(lights_file, cloud_file), (), ...], "00N060W": [...]}

this format is used to iterate over all year-months of a single tile before
moving on to a different tile, so that we only have to store the cumulative
cloud count for a single tile at a time
"""


year_months = [i for i in os.listdir(data_path) if i.startswith(tuple(map(str, years)))]

print "Processing year-months: {}".format(year_months)
tile_files = {}

for ym in year_months:
    for pth, dirs, files in os.walk(os.path.join(data_path, ym)):
        for f in files:

            if f.endswith('.avg_rade9.tif'):

                tile_id = os.path.basename(f).split("_")[3]

                lights_file = os.path.join(pth, f)

                cloud_name = os.path.basename(lights_file).split('.')[0] + '.cf_cvg.tif'
                cloud_file = os.path.join(os.path.dirname(lights_file), cloud_name)

                if not os.path.isfile(cloud_file):
                    raise Exception("File does not exist ({0})".format(cloud_file))

                if not tile_id in tile_files:
                    tile_files[tile_id] = []

                tile_files[tile_id].append((lights_file, cloud_file))

print "\nFiles:"
print(tile_files)

# -----------------------------------------------------------------------------


def make_dir(path):
    """Make directory.

    Args:
        path (str): absolute path for directory

    Raise error if error other than directory exists occurs.
    """
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def prepare_tiles(tile_id, file_tuples):
    """
    process all monthly tiles for a given tile section at the same time
    use cloud free layer and 'cf_minimum' threshold to generate mask
    """
    print "Processing Tile: {0} ({1} layers)".format(
        tile_id, len(file_tuples))

    tile_cloud_count_array = None
    tile_profile = None

    for lights_path, cloud_path in file_tuples:

        print(lights_path)

        lights_basename = os.path.basename(lights_path)[:-4]

        # create output folder
        src_dirname = lights_path.split("/")[-2]
        dst_dir = os.path.join(out_path, src_dirname)

        make_dir(dst_dir)

        # build output path for filtered lights
        out_lights = os.path.join(
            dst_dir, lights_basename) + ".tif"

        # read in lights and cloud data
        src_lights = rasterio.open(lights_path)
        array_lights = src_lights.read(1)
        profile_lights = src_lights.profile

        src_cloud = rasterio.open(cloud_path)
        array_cloud = src_cloud.read(1)
        profile_cloud = src_cloud.profile

        # set profile for tile using first layer
        if tile_profile is None:
            tile_profile = profile_cloud.copy()

        # check if cloud and ntl has same dimension
        # raise error if not same dimension
        if array_cloud.shape != array_lights.shape:
            raise Exception('Cloud/ntl different shapes ', cloud_path)

        # generate mask
        mask = (array_cloud <= cf_minimum).astype('uint16')

        # ---------------------------------

        # build output path for mask
        out_mask = os.path.join(dst_dir, lights_basename.split('.')[0]) + ".mask.tif"

        make_dir(os.path.dirname(out_mask))

        with rasterio.open(out_mask, 'w', **profile_cloud) as export_img:
            export_img.write(mask, 1)

        # ---------------------------------

        # create masked lights layer using cloud mask
        masked_lights = np.ma.masked_array(array_lights, mask=mask)

        # fill with non zero (eg: -9999) make sure matches nodata in profile
        masked_lights_array = masked_lights.filled(-9999)

        profile_lights['nodata'] = -9999

        # converting negative radiance to None data
        # masked_lights_array = np.where(
        #     masked_lights_array < 0, -9999, masked_lights_array)

        make_dir(os.path.dirname(out_lights))
        with rasterio.open(out_lights, 'w', **profile_lights) as export_img:
            export_img.write(masked_lights_array, 1)

        # ---------------------------------

        # update tile cloud array
        if tile_cloud_count_array is None:
            tile_cloud_count_array = mask
        else:
            tile_cloud_count_array = np.add(tile_cloud_count_array, mask)


    #  --------------------------------

    # build tile cloud summary path and export cumulative mask count for tile
    tile_output = os.path.join(cloud_count_path, "tiles", tile_id + "_cloud_mask_count.tif")

    make_dir(os.path.dirname(tile_output))
    with rasterio.open(tile_output, 'w', **tile_profile) as export_tile:
        export_tile.write(tile_cloud_count_array, 1)



mode = "parallel"
# mode = "serial"

if mode == "parallel":
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    c = rank
    while c < len(tile_files):

        try:
            tile_id = tile_files.keys()[c]
            prepare_tiles(tile_id, tile_files[tile_id])
        except Exception as e:
            print "Error processing tile section: {0}".format(c)
            raise

        c += size

    comm.Barrier()

elif mode == "serial":

    for c in range(len(tile_files)):
        tile_id = tile_files.keys()[c]
        prepare_tiles(tile_id, tile_files[tile_id])

else:
    raise Exception("Invalid `mode` value for script.")




# -----------------------------------------------------------------------------


if mode == "serial" or rank == 0:

    # mosaic the tiles for the cumulative cloud mask count

    cm_mosaic_path = os.path.join(cloud_count_path, "cloud_mask_count_mosaic.tif")


    # recreate a list of the cumulate cloud count tile outputs
    cm_tiles = [
        os.path.join(cloud_count_path, "tiles", tile_id + "_cloud_mask_count.tif")
        for tile_id in tile_files.keys()
    ]


    cm_tiles = [rasterio.open(tile) for tile in cm_tiles]

    cm_array, cm_transform = tile_mosaic(cm_tiles)

    cm_profile = cm_tiles[0].profile


    if 'affine' in cm_profile:
            cm_profile.pop('affine')

    cm_profile["transform"] = cm_transform
    cm_profile['height'] = cm_array.shape[1]
    cm_profile['width'] = cm_array.shape[2]
    cm_profile['driver'] = 'GTiff'

    make_dir(os.path.dirname(cm_mosaic_path))
    with rasterio.open(cm_mosaic_path, 'w', **cm_profile) as cm_dst:
        cm_dst.write(cm_array)
