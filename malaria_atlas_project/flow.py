
from prefect import flow

from main import MalariaAtlasProject


@flow
def malaria_atlas_project(raw_dir, output_dir, years, dataset, overwrite, backend, run_parallel, max_workers):

    class_instance = MalariaAtlasProject(raw_dir, output_dir, years, dataset, overwrite)

    class_instance.run(backend=backend, task_runner="dask", run_parallel=run_parallel, max_workers=max_workers)
