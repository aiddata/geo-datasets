"""

1. Go to https://landscan.ornl.gov and register for account if you do not have one already (may take several days for them to approve)
2. Then go to https://landscan.ornl.gov/landscan-datasets and open the Developer Tools for your browser (e.g., press F12 in Chrome)
3. Select the "Application" tab in Developer Tools
4. In the "Storage" section of the left hand side menu click "Cookies" and then select "https://landscan.ornl.gov"
5. In the main area you should now see a table of cookies with a "Name" and "Value" column.
6. Copy the "Name" that starts with "SESS" and replace the "cookie_name" variable value in the code below
7. Copy the "Value" corresponding to the above Name and replace the "cookie_value" variable value in the code below
8. This cookie has an expiration date (<1 month) so you will need to retrieve a new cookie at some point in the future
9. Enter your username and password into the variables in the code below
10. Set year range (or define list of years) to download
11. Set the output directory
12. Define whether to run in parallel and maximum number of workers

Note: Do not share your username, password, or cookie combination.

If running interactively, make sure your working directory contains utility.py
When calling the script (e.g., `python prepare_data.py) it will automatically set the working directory.

"""

import os
import sys

import glob
import pandas as pd

# ensure correct working directory when running as a batch parallel job
# in all other cases user should already be in project directory
if not hasattr(sys, 'ps1'):
    os.chdir(os.path.dirname(__file__))

import utility


years = list(range(2000, 2020))


raw_dir = "/sciclone/aiddata10/REU/geo/raw/landscan/population"
data_dir = "/sciclone/aiddata10/REU/geo/data/rasters/landscan/population"


parallel = True
max_workers = 16

# replace with your cookie information
cookie_name = "COOKIE_NAME"
cookie_value = "COOKIE_VALUE"

# replace with your user credentials
username = "YOUR_USERNAME"
password = "YOUR_PASSWORD"


# update utility session with authentication and cookie for each worker
utility.session.auth = (username, password)
utility.session.cookies.set(cookie_name, cookie_value)



if __name__ == "__main__":

    timestamp = utility.get_current_timestamp('%Y_%m_%d_%H_%M')

    # test connection
    test_request = utility.session.get("https://landscan.ornl.gov/landscan-datasets")
    test_request.raise_for_status()


    print("Running download")
    download_dir = os.path.join(raw_dir, "compressed")
    os.makedirs(download_dir, exist_ok=True)

    qlist = [(year, os.path.join(download_dir, f"LandScan_Global_{year}.zip")) for year in years]
    download_results = utility.run_tasks(utility.download_file, qlist, parallel, max_workers=max_workers, chunksize=1)

    print("Saving download results")
    download_results_df = pd.DataFrame(download_results, columns=["status", "message", "year", "result"])
    download_results_df["year"] = download_results_df["year"].apply(lambda x: x[0])
    download_results_df["downloaded_path"] = download_results_df.year.apply(lambda x: os.path.join(download_dir, f"LandScan_Global_{x}.zip"))

    download_results_path = os.path.join(raw_dir, f"download_results_{timestamp}.csv")
    download_results_df.to_csv(download_results_path, index=False)


    # unzip
    print("Running extract")
    extract_dir = os.path.join(raw_dir, "uncompressed")
    os.makedirs(extract_dir, exist_ok=True)

    qlist = [ ( os.path.join(download_dir, x), os.path.join(extract_dir, x[:-4]) ) for x in os.listdir(download_dir) ]
    extract_results = utility.run_tasks(utility.unzip_file, qlist, parallel, max_workers=max_workers, chunksize=1)

    print("Saving extract results")
    extract_results_df = pd.DataFrame(extract_results, columns=["status", "message", "args", "drop"])
    extract_results_df["zip"] = extract_results_df["args"].apply(lambda x: x[0])
    extract_results_df["extract"] = extract_results_df["args"].apply(lambda x: x[1])
    extract_results_df.drop(["args", "drop"], axis=1, inplace=True)

    extract_results_path = os.path.join(raw_dir, f"extract_results_{timestamp}.csv")
    extract_results_df.to_csv(extract_results_path, index=False)


    # convert from esri grid format to geotiff
    print("Running conversion")
    os.makedirs(data_dir, exist_ok=True)

    qlist = [ ( x, os.path.join(data_dir, os.path.basename(x)+'.tif') ) for x in glob.glob(extract_dir + '/*/lspop*') if os.path.isdir(x)]
    conversion_results = utility.run_tasks(utility.convert_esri_grid_to_geotiff, qlist, parallel, max_workers=max_workers, chunksize=1)

    print("Saving conversion results")
    conversion_results_df = pd.DataFrame(conversion_results, columns=["status", "message", "args", "drop"])
    conversion_results_df["extract"] = conversion_results_df["args"].apply(lambda x: x[0])
    conversion_results_df["geotiff"] = conversion_results_df["args"].apply(lambda x: x[1])
    conversion_results_df.drop(["args", "drop"], axis=1, inplace=True)

    conversion_results_path = os.path.join(raw_dir, f"conversion_results_{timestamp}.csv")
    conversion_results_df.to_csv(conversion_results_path, index=False)












timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

# test connection
test_request = session.get("https://landscan.ornl.gov/landscan-datasets")
test_request.raise_for_status()


print("Running download")
download_dir = os.path.join(raw_dir, "compressed")
os.makedirs(download_dir, exist_ok=True)

qlist = [(year, os.path.join(download_dir, "compressed", f"LandScan_Global_{year}.zip")) for year in years]
download_results = run_tasks(download_file, qlist, parallel, max_workers=max_workers, chunksize=1)

print("Saving download results")
download_results_df = pd.DataFrame(download_results, columns=["status", "message", "year", "result"])
download_results_df["year"] = download_results_df["year"].apply(lambda x: x[0])
download_results_df["downloaded_path"] = download_results_df.year.apply(lambda x: os.path.join(download_dir, f"LandScan_Global_{x}.zip"))

download_results_path = os.path.join(raw_dir, f"download_results_{timestamp}.csv")
download_results_df.to_csv(download_results_path, index=False)


# unzip
print("Running extract")
extract_dir = os.path.join(raw_dir, "uncompressed")
os.makedirs(extract_dir, exist_ok=True)

qlist = [ ( os.path.join(download_dir, x), extract_dir ) for x in os.listdir(download_dir) ]
extract_results = run_tasks(unzip_file, qlist, parallel, max_workers=max_workers, chunksize=1)

print("Saving extract results")
extract_results_df = pd.DataFrame(extract_results, columns=["status", "message", "args", "drop"])
extract_results_df["zip"] = extract_results_df["args"].apply(lambda x: x[0])
extract_results_df["extract"] = extract_results_df["args"].apply(lambda x: x[1])
extract_results_df.drop(["args", "drop"], axis=1, inplace=True)

extract_results_path = os.path.join(raw_dir, f"extract_results_{timestamp}.csv")
extract_results_df.to_csv(extract_results_path, index=False)


# convert from esri grid format to geotiff
print("Running conversion")
os.makedirs(data_dir, exist_ok=True)

qlist = [ ( x, os.path.join(data_dir, os.path.basename(x)+'.tif') ) for x in glob.glob(extract_dir + '/*/lspop*') if os.path.isdir(x)]
conversion_results = run_tasks(convert_esri_grid_to_geotiff, qlist, parallel, max_workers=max_workers, chunksize=1)

print("Saving conversion results")
conversion_results_df = pd.DataFrame(conversion_results, columns=["status", "message", "args", "drop"])
conversion_results_df["extract"] = conversion_results_df["args"].apply(lambda x: x[0])
conversion_results_df["geotiff"] = conversion_results_df["args"].apply(lambda x: x[1])
conversion_results_df.drop(["args", "drop"], axis=1, inplace=True)

conversion_results_path = os.path.join(raw_dir, f"conversion_results_{timestamp}.csv")
conversion_results_df.to_csv(conversion_results_path, index=False)


