
import os
import requests
import time
import datetime
import json



def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp

def make_dirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

version = 'v4'

# generic format for latest data
base_api_url = "https://www.geoboundaries.org/api/current/gbOpen/ALL/ALL/"

# format for major releases (yet to be implemented)
# base_api_url = "https://www.geoboundaries.org/api/v4/gbOpen/ALL/ALL/"
# base_api_url = "https://www.geoboundaries.org/api/{}/gbOpen/ALL/ALL/".format(version)

base_dir = "/sciclone/aiddata10/REU/geo/data/boundaries"


date_str = get_current_timestamp('%Y_%m_%d')

output_base_dir = os.path.join(base_dir, "geoboundaries", version)

r = requests.get(base_api_url)

r_json = r.json()


# iterate over boundary files
for metadata in r_json:

    print("Downloading {}".format(metadata["boundaryID"]))

    meta = metadata.copy()

    # --------------
    # file prep

    boundary_basename = "{}_{}".format(meta["boundaryISO"], meta["boundaryType"])

    boundary_dir = os.path.join(output_base_dir, boundary_basename)

    make_dirs(boundary_dir)

    # --------------
    # metadata

    # add info to meta (version, download date, etc)
    meta['gq_version'] = version
    meta['gq_download_date'] = date_str

    # save metadata as json
    meta_file = open(boundary_dir + "/metadata.json", "w")
    json.dump(meta, meta_file, indent=4)
    meta_file.close()

    # --------------
    # boundary

    # get main geojson download path
    dlPath = meta['gjDownloadURL']

    # all features for boundary files
    try:
        geoBoundary = requests.get(dlPath).json()
    except:
        original_str = "raw.githubusercontent.com"
        replacement_str = "media.githubusercontent.com/media"
        lfs_dlPath = dlPath.replace(original_str, replacement_str)
        try:
            geoBoundary = requests.get(lfs_dlPath).json()
        except Exception as e:
            print("-------------------------------")
            print(dlPath)
            print(lfs_dlPath)
            print(meta)
            print(e)

        # save geojson
        fname = "{}.geojson".format(boundary_basename)
        geo_file = open(os.path.join(boundary_dir, fname), "w")
        json.dump(geoBoundary, geo_file)
        geo_file.close()



