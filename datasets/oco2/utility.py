import datetime
import hashlib
import os
import time
import warnings

import requests
from bs4 import BeautifulSoup


def download_file(url, local_filename):
    """Download a file from url to local_filename

    Downloads in chunks
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def get_md5sum_from_xml_url(url, field):
    """Read the md5sum from an xml file

    - XML file provided as url
    - XML field containing md5sum must be provided
    """
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    md5sum = soup.find(field).text
    return md5sum


def calc_md5sum(path):
    """Calculate the md5sum for a file"""
    with open(path, "rb") as f:
        md5sum = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            md5sum.update(chunk)
            chunk = f.read(8192)
        return md5sum.hexdigest()


def find_files(url, ext=""):
    """Find all files on a webpage

    Option matching on string at end of links founds

    Returns list of complete (absolute) links
    """
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    urllist = [
        url + "/" + node.get("href")
        for node in soup.find_all("a")
        if node.get("href").endswith(ext)
    ]
    return urllist


def file_exists(path):
    return os.path.isfile(path)


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = "%Y_%m_%d_%H_%M"
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def _task_wrapper(func, args):
    try:
        func(*args)
        return (0, "Success", args)
    except Exception as e:
        return (1, repr(e), args)


def run_tasks(func, flist, mode, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)

    wrapper_list = [(func, i) for i in flist]

    if mode == "parallel":
        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        from mpi4py.futures import MPIPoolExecutor

        if max_workers is None:
            if "OMPI_UNIVERSE_SIZE" not in os.environ:
                raise ValueError(
                    "Mode set to parallel but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found"
                )

            max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
            warnings.warn(
                f"Mode set to parallel but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})"
            )

        with MPIPoolExecutor(max_workers=max_workers) as executor:
            # results_gen = executor.starmap(func, flist, chunksize=chunksize)
            results_gen = executor.starmap(
                _task_wrapper, wrapper_list, chunksize=chunksize
            )

        results = list(results_gen)

    else:
        results = []
        # for i in flist:
        # results.append(func(*i))
        for i in wrapper_list:
            results.append(_task_wrapper(*i))

    return results
