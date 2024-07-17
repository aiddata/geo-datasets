import logging
import tomllib
from configparser import ConfigParser
from pathlib import Path
from typing import Literal, Optional, Union

from pydantic import BaseModel


class RunParameters(BaseModel):
    """
    This is a pydantic BaseModel that represents the run
    parameters for a Dataset. This model is consumed by
    Dataset.run() as settings for how to run the Dataset.
    """
    backend: Literal["local", "mpi", "prefect"] = "prefect"
    task_runner: Literal[
        "concurrent",
        "dask",
        "hpc",
        "kubernetes",
        "sequential",
    ] = "concurrent"
    run_parallel: bool = True
    max_workers: Optional[int] = 4
    bypass_error_wrapper: bool = False
    threads_per_worker: Optional[int] = 1
    # cores_per_process: Optional[int] = None
    chunksize: int = 1
    log_dir: str
    logger_level: int = logging.INFO
    retries: int = 3
    retry_delay: int = 5
    conda_env: str = "geodata38"


class BaseDatasetConfiguration(BaseModel):
    """
    This is the class that should be imported into
    `main.py` files within dataset directories, and
    built upon with Dataset-specific parameters.
    Common examples are `overwrite_download`,
    `overwrite_processing`, or `year_list`.
    """
    run: RunParameters


def get_config(
    model: BaseDatasetConfiguration, config_path: Union[Path, str] = "config.toml"
):
    """
    Load the configuration for a Dataset.

    This function reads a TOML configuration
    file (usually `config.toml`) out of the
    same directory as the `main.py` file, and
    returns a `BaseDatasetConfiguration` model
    filled in with the values from that
    configuration file.
    """
    config_path = Path(config_path)
    if config_path.exists():
        with open(config_path, "rb") as src:
            return model.model_validate(tomllib.load(src))
    else:
        return FileNotFoundError("No TOML config file found for dataset.")
