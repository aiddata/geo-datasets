


import os
import rasterio
import numpy as np


# -----------------------------------------------------------------------------


# tile_id_list = ["00N060E", "00N060W", "00N180W", "75N060E", "75N060W", "75N180W"]

data_path = "/sciclone/aiddata10/REU/geo/raw/viirs/monthly_vcmcfg_dnb_composites_v10"
out_path = "/sciclone/aiddata10/REU/scr/viirs"

# minimum cloud free day threshold
cf_minimum = 2


# -----------------------------------------------------------------------------


tile_files = {}

for pth, dirs, files in os.walk(data_path):
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



# -----------------------------------------------------------------------------



for tile_id, file_tuples in tile_files.iteritems():

    print "Processing Tile: {0} ({1} layers)".format(tile_id, len(file_tuples))

    tile_cloud_count_array = None
    tile_profile = None

    for lights_path, cloud_path in file_tuples:

        print lights_path

        # create output folder
        src_dirname = lights_path.split("/")[-2]
        dst_dir = os.path.join(out_path, src_dirname)

        if not os.path.isdir(dst_dir):
            os.makedirs(dst_dir)

        # get output name
        out_lights = os.path.join(dst_dir, os.path.basename(lights_path)) + ".tif"

        src_lights = rasterio.open(lights_path)
        array_lights = src_lights.read(1)
        profile_lights = src_lights.profile

        src_cloud = rasterio.open(cloud_path)
        array_cloud = src_cloud.read(1)
        profile_cloud = src_cloud.profile

        if tile_profile is None:
            tile_profile = profile_cloud.copy()

        # check if cloud and ntl has same dimension
        # raise error if not same dimension
        if array_cloud.shape != array_lights.shape:
            raise ('The shape of cloud image is not same with ntl image', cloud_path)

        mask = (array_cloud <= cf_minimum)

        out_mask = os.path.join(dst_dir, os.path.basename(lights_path)) + "_mask.tif"

        with rasterio.open(out_mask, 'w', **profile_cloud) as export_img:
            export_img.write(mask.astype('int16'), 1)


        # ---------------------------------

        masked_lights = np.ma.masked_array(array_lights, mask=mask)

        # fill with non zero (eg: -9999) make sure matches nodata in profile
        masked_lights_array = masked_lights.filled(0)

        with rasterio.open(out_lights, 'w', **profile_lights) as export_img:
            export_img.write(masked_lights_array, 1)

        # ---------------------------------


        if tile_cloud_count_array is None:
            tile_cloud_count_array = mask.astype('int16')

        else:
            tile_cloud_count_array = np.add(tile_cloud_count_array, mask.astype('int16'))


    #  --------------------------------

    tile_output = os.path.join(out_path, tile_id) + "_cloud_mask_count.tif"

    with rasterio.open(tile_output, 'w', **tile_profile) as export_tile:

        export_tile.write(tile_cloud_count_array.astype(rasterio.uint16), 1)




