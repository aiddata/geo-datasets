


import os
import rasterio
import numpy as np



tile_lists = {}
"""

tile_lists = {
    "00N060E": [
        (yearmonth, cloud_path, avg_path),
        (yearmonth, cloud_path, avg_path),
    ],
    "00N060W": [],
    "00N180W": [],
    "75N060E": [],
    "75N060W": [],
    "75N180W": []

}

"""

def get_avg_cloud(path):
    avg_list = list()
    cloud_list = list()
    for pth, dirs, files in os.walk(path):
        for f in files:
            if f.endswith('.tif') and f.split('.')[1] == 'avg_rade9':
                avg_file = os.path.join(pth, f)
                avg_list.append(avg_file)

                cloud_nm = os.path.basename(avg_file).split('.')[0] + '.' + 'cf_cvg' + '.tif'
                cloud_file = os.path.join(os.path.dirname(avg_file), cloud_nm)

                if not os.path.isfile(cloud_file):
                    raise Exception("File does not exist ({0})".format(cloud_file))

                cloud_list.append(cloud_file)

    imgs = zip(avg_list, cloud_list)

    return imgs




def read_mask(img, cut_day):
    img_im = rasterio.open(img)

    img_profile = img_im.profile

    img_array = img_im.read(1)
    img_mask = (img_array > cut_day)

    return [img_mask, img_profile]




data_path = "/sciclone/aiddata10/REU/geo/raw/viirs/monthly_vcmcfg_dnb_composites_v10"
out_path = "/sciclone/data10/zlv/maskout"

# minimum cloud free day threshold
cf_minimum = 2

file_tuples = get_avg_cloud(data_path)



# create maskoutted ntl images
for avg_path, cloud_path in file_tuples:

        # create output folder
        src_dirname = avg_path.split("/")[-2]
        dst_dir = os.path.join(out_path, src_dirname)

        if not os.path.isdir(dst_dir):
            os.mkdir(dst_dir)


        # get output name
        out_avg = os.path.join(dst_dir, os.path.basename(avg_path)) + ".tif"


        src_cloud = rasterio.open(cloud_path)
        array_cloud = src_cloud.read(1)

        src_avg = rasterio.open(avg_path)
        array_avg = src_avg.read(1)


        # check if cloud and ntl has same dimension
        # raise error if not same dimension
        if array_cloud.shape != array_avg.shape:
            raise ('The shape of cloud image is not same with ntl image', cloud_path)

        else:

            avg_masked = np.ma.masked_array(array_avg, mask=(array_cloud > cf_minimum))

            # fill with non zero (eg: -9999) make sure matches nodata in profile
            avg_masked_array = avg_masked.filled(0)

            profile = src_avg.profile

            with rasterio.open(out_avg, 'w', **profile) as export_img:

                export_img.write(avg_masked_array, 1)





tile_data = {}
tile_profiles = {}

for avg_path, cloud_path in file_tuples:

    # can be: ["00N060E", "00N060W", "00N180W", "75N060E", "75N060W", "75N180W"]
    loc = os.path.basename(cloud_path).split("_")[3]

    m_array, m_prof = read_mask(cloud_path, cf_minimum)

    if loc not in tile_data.keys():

        tile_data[loc] = m_array
        tile_profiles[loc] = m_prof

    else:

        tile_data[loc] = np.add(dict_img[loc][0], m_array)




for loc in tile_data.keys():

    mask_sum_dir = join(out_path, loc) + ".tif"

    out_prof = tile_profiles[loc]

    print out_prof

    with rasterio.open(mask_sum_dir, 'w', **out_prof) as sum_cloud:

        sum_cloud.write(dict_img[loc].astype(rasterio.uint16), 1)


























