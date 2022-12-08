from prefect import flow

from main import ESALandcover

years = [int(y) for y in range(1998, 2021)]

@flow
def esa_landcover(
    raw_dir: str="/sciclone/aiddata10/REU/geo/raw/esa_landcover",
    output_dir: str="/sciclone/aiddata10/REU/geo/data/rasters/esa_landcover",
    years=years,
    backend="prefect",
    task_runner=None,
    run_parallel=True,
    max_workers=None,
    overwrite=True):

    class_instance = ESALandcover(raw_dir, output_dir, years, overwrite)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers)
