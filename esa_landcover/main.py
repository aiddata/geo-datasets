"""
Download and prepare data
"""
import os

from download import download_data
from prepare import prepare_data

project_kwargs = {

    # download directory
    "raw_dir": "/sciclone/aiddata10/REU/geo/raw/esa_landcover",

    # final data directory
    "output_dir": "/sciclone/aiddata10/REU/geo/data/rasters/esa_landcover",

    # accepts int or str
    "years": range(1992, 2021),

    "backend": "prefect",

    "run_parallel": True,

    "max_workers": 30,
}

download_kwargs = {
    "v207_years": range(1992, 2016),
    "v211_years": range(2016, 2021),
}

@flow
def process_dataset():
    os.makedirs(project_kwargs["output_dir"], exist_ok=True)
    download_data(**project_kwargs, **download_kwargs)
    prepare_data(**project_kwargs)

if __name__ == "__main__":
    process_dataset()
