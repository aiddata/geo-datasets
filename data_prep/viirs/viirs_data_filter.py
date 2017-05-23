


import rasterio
from os import listdir, walk
from os.path import isfile, join, basename, dirname, isdir
import numpy as np
import os


def get_files(path):
    file_lst = list()
    for pth, dirs, files in walk(path):
        for f in files:
            if '.tif' in f:
                file = join(pth, f)
                file_lst.append(file)
    return file_lst


def get_avg_cloud(path):
    avg_list = list()
    cloud_list = list()
    for pth, dirs, files in walk(path):
        for f in files:
            if ('.tif' in f) and (f.split('.')[1] == 'avg_rade9'):
                avg_file = join(pth, f)
                avg_list.append(avg_file)
                cloud_nm = basename(avg_file).split('.')[0] + '.' + 'cf_cvg' + '.tif'
                cloud_file = join(dirname(avg_file), cloud_nm)
                cloud_list.append(cloud_file)

    imgs = zip(avg_list, cloud_list)
    return imgs



def generate_img(avg_img, cloud_img, out_avg, cut_day):
    # check if cloud and ntl has same dimension
    # raise error if not same dimension

    src_cloud = rasterio.open(cloud_img)
    array_cloud = src_cloud.read(1)
    cloud_shape = array_cloud.shape

    src_avg = rasterio.open(avg_img)
    array_avg = src_avg.read(1)
    avg_shape = array_avg.shape

    if cloud_shape != avg_shape:
        raise ('The shape of cloud image is not same with ntl image', cloud_img)
    else:
        cloud_mask = np.where(array_cloud > cut_day, 0, 1)
        print "avg none mask mean", array_avg.mean()
        avg_masked = np.ma.masked_array(array_avg, mask=cloud_mask)
        avg_masked_array = avg_masked.filled(0)
        print "avg_masked_mean", avg_masked.mean()
        avg_mask_mean = avg_masked_array.mean()

        profile = src_avg.profile

        with rasterio.open(out_avg, 'w', **profile) as export_img:
            export_img.write(avg_masked_array, 1)

    #return cloud_mask

def get_folder(path):
    #for pth, dir, file in walk(path):
    #   print dir
    folder_name = path.split("/")[-2] # get folder name
    return folder_name


def read_mask(img, cut_day):
    img_im = rasterio.open(img)
    img_profile = img_im.profile
    img_array = img_im.read(1)
    img_mask = np.where(img_array > cut_day, 0, 1)
    return [img_mask, img_profile]




data_path = r"/sciclone/aiddata10/REU/geo/raw/viirs/monthly_vcmcfg_dnb_composites_v10"
out_path = r"/sciclone/data10/zlv/maskout"
cut = 2


a = get_avg_cloud(data_path)

# create maskoutted ntl images
for i in range(0,len(a)):
    if i == 0:
        avg = a[i][0]
        cloud = a[i][1]
        # create output folder
        folder_nm = get_folder(avg)
        newfolder = join(out_path, folder_nm)

        if not isdir(join(out_path, folder_nm)):
            os.mkdir(join(out_path, folder_nm))

        # get output name
        out_avg = join(newfolder, basename(avg))

        generate_img(avg, cloud, out_avg, cut)
    else:
        avg = a[i][0]
        cloud = a[i][1]
        # create output folder
        folder_nm = get_folder(avg)
        newfolder = join(out_path, folder_nm)

        if not isdir(join(out_path, folder_nm)):
            os.mkdir(join(out_path, folder_nm))

        # get output name
        out_avg = join(newfolder, basename(avg))

        generate_img(avg, cloud, out_avg, cut)




# generate mask cloud
latlons = ["00N060E", "00N060W", "00N180W", "75N060E", "75N060W", "75N180W"]

dict_img = dict()

#b = a[0:7]

for j in a:
    loc = basename(j[1]).split("_")[3]
    if loc not in dict_img.keys():
        m = read_mask(j[1], cut)
        m_array = m[0]
        m_prof = m[1]

        prof_key = loc + "_prof"
        dict_img[loc] = m_array
        dict_img[prof_key] = m_prof

    else:
        m_array = read_mask(j[1], cut)[0]
        dict_img[loc] = np.add(dict_img[loc][0], m_array)


for key in dict_img.keys():
    if "prof" not in key:
        mask_sum_dir = join(out_path, key) + ".tif"
        k_prfl = key + "_prof"
        out_prof = dict_img[k_prfl]
        print out_prof
        with rasterio.open(mask_sum_dir, 'w', **out_prof) as sum_cloud:
            sum_cloud.write(dict_img[key].astype(rasterio.uint16), 1)


























