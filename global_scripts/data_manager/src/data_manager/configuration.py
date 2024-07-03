import logging
from typing import Literal, Optional, Union

from pydantic import BaseModel


class RunParameters(BaseModel):
    backend: Union[Literal["local"], Literal["mpi"], Literal["prefect"]] = "prefect"
    task_runner: Union[
        Literal["concurrent"],
        Literal["dask"],
        Literal["hpc"],
        Literal["kubernetes"],
        Literal["sequential"],
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
