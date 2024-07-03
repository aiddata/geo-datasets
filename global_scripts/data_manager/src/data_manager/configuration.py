import logging
import tomllib
from configparser import ConfigParser
from pathlib import Path
from typing import Literal, Optional, Union

from pydantic import BaseModel


class RunParameters(BaseModel):
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
    run: RunParameters


def get_config(
    model: BaseDatasetConfiguration, config_path: Union[Path, str] = "config.toml"
):
    config_path = Path(config_path)
    if config_path.exists():
        with open(config_path, "rb") as src:
            return model.model_validate(tomllib.load(src))
    else:
        return FileNotFoundError("No TOML config file found for dataset.")
