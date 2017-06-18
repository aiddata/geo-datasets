
import os
import glob
import rasterio
from rasterio.merge import merge as scene_mosaic



def run_mosaic(i):

    print "Running {0}".format(i)

    tile_dir = os.path.join(tile_data, i)
    tile_list = glob.glob(tile_dir + "/*.avg_rade9.tif")

    if len(tile_list) != 6:
        raise Exception("Bad tile count ({0})".format(len(tile_list)))


    mosaic_scenes = [rasterio.open(path) for path in tile_list]
    mosaic_profile = mosaic_scenes[0].profile

    mosaic_array, transform = scene_mosaic(mosaic_scenes)

    for i in mosaic_scenes: i.close()

    if 'affine' in mosaic_profile:
        mosaic_profile.pop('affine')

    mosaic_profile["transform"] = transform
    mosaic_profile['height'] = mosaic_array.shape[1]
    mosaic_profile['width'] = mosaic_array.shape[2]
    mosaic_profile['driver'] = 'GTiff'

    mosaic_output_path = os.path.join(mosaic_data, "i.tif")

    mosaic = rasterio.open(mosaic_output_path, 'w', **mosaic_profile)
    mosaic.write(mosaic_array)
    mosaic.close()




tile_data = "/sciclone/aiddata10/REU/geo/raw/viirs/vcmcfg_dnb_composites_v10/filtered_monthly"

mosaic_data = "/sciclone/aiddata10/REU/geo/data/rasters/external/global/viirs/vcmcfg_dnb_composites_v10/monthly"


mode = "parallel"
# mode = "serial"


qlist = os.listdir(tile_data)


if mode == "parallel":
    from mpi4py import MPI
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    c = rank
    while c < len(qlist):

        try:
            run_mosaic(qlist[c])
        except Exception as e:
            print "Error processing tile section: {0}".format(c)
            raise

        c += size

    comm.Barrier()

elif mode == "serial":

    for c in range(len(qlist)):
        run_mosaic(qlist[c])

else:
    raise Exception("Invalid `mode` value for script.")




