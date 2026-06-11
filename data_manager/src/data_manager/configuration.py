import logging
import tomllib
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
    """
    The backend to run the dataset on.
    Most common values are "sequential", and "concurrent"
    """
    run_parallel: bool = True
    """
    Whether or not to run the Dataset in parallel.
    """
    max_workers: Optional[int] = 4
    """
    Maximum number of concurrent tasks that may be run for this Dataset.
    This may be overridden when calling `Dataset.run_tasks()`
    """
    bypass_error_wrapper: bool = False
    """
    If set to `True`, exceptions will not be caught when running tasks, and will instead stop execution of the entire dataset.
    This can be helpful for quickly debugging a dataset, especially when it is running sequentially.
    """
    threads_per_worker: Optional[int] = 1
    """
    `threads_per_worker` passed through to the DaskCluster when using the dask task runner.
    """
    # cores_per_process: Optional[int] = None
    chunksize: int = 1
    """
    Sets the chunksize for pools created for concurrent or MPI task runners.
    """
    log_dir: str
    """
    Path to directory where logs for this Dataset run should be saved.
    This is the only run parameter without a default, so it must be set in a Dataset's configuration file.
    """
    logger_level: int = logging.INFO
    """
    Minimum log level to log.
    For more information, see the [relevant Python documentation](https://docs.python.org/3/library/logging.html#logging-levels).
    """
    retries: int = 3
    """
    Number of times to retry each task before giving up.
    This parameter can be overridden per task run when calling `Dataset.run_tasks()`
    """
    retry_delay: int = 5
    """
    Time in seconds to wait between task retries.
    This parameter can be overridden per task run when calling `Dataset.run_tasks()`
    """
    conda_env: str = "geodata38"
    """
    Conda environment to use when running the dataset.
    **Deprecated because we do not use this in the new Prefect/Kubernetes setup**
    """


class BaseDatasetConfiguration(BaseModel):
    """
    This is the class that should be imported into
    `main.py` files within dataset directories, and
    built upon with Dataset-specific parameters.
    Common examples are `overwrite_download`,
    `overwrite_processing`, or `year_list`.
    """

    run: RunParameters
    """
    A `RunParameters` model that defines how this model should be run.
    This is passed into the `Dataset.run()` function.
    """


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

    Parameters:
        model: The model to load the configuration values into. This should nearly always be a Dataset-specific model defined in `main.py` that inherits `BaseDatasetConfiguration.
        config_path: The relative path to the TOML configuration file. It's unlikely this parameter should ever be changed from its default.
    """
    config_path = Path(config_path)
    if config_path.exists():
        with open(config_path, "rb") as src:
            return model.model_validate(tomllib.load(src))
    else:
        return FileNotFoundError("No TOML config file found for dataset.")
