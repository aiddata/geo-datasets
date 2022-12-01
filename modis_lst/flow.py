from prefect import flow

from main import MODISLandSurfaceTemp


@flow
def modis_lst(raw_dir, output_dir, username, password, years, backend, task_runner, run_parallel, max_workers):

    class_instance = MODISLandSurfaceTemp(raw_dir, output_dir, username, password, years)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers)
