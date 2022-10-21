import os
import shutil
import requests
from zipfile import ZipFile


def download_file(url, local_filename):
    """Download a file from url to local_filename
    Downloads in chunks
    """
    with requests.get(url, stream=True, verify=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                f.write(chunk)


def manage_download(url, local_filename, overwrite=False):
    """download individual file using session created
    this needs to be a standalone function rather than a method
    of SessionWithHeaderRedirection because we need to be able
    to pass it to our mpi4py map function
    """
    max_attempts = 5
    if os.path.isfile(local_filename) and not overwrite:
        print(f'Download Exists: {url}')
    else:
        attempts = 1
        while attempts <= max_attempts:
            try:
                download_file(url, local_filename)
                print("Downloaded: {url}")
            except Exception as e:
                attempts += 1
                if attempts > max_attempts:
                    raise e


def copy_files(data_zip, cur_loc, new_loc, overwrite=False):
    if os.path.isfile(new_loc) and not overwrite:
        return new_loc

    else:
        try:
            with ZipFile(data_zip) as myzip:
                with myzip.open(cur_loc) as source:
                    with open(new_loc, "wb") as target:
                        shutil.copyfileobj(source, target)

            if not os.path.isfile(target):
                raise Exception("File extracted but not found at destination")

            return new_loc
        except Exception as e:
            raise e


