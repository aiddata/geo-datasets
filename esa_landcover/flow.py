import sys, os

from prefect import flow
from prefect.filesystems import GitHub

block_name = "geo-datasets-github-esa"
GitHub.load(block_name).get_directory('global_scripts')

# sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'esa_landcover'))


from main import ESALandcover


@flow
def esa_landcover(raw_dir, output_dir, years, overwrite, backend, task_runner, run_parallel,  max_workers, log_dir):

    class_instance = ESALandcover(raw_dir=raw_dir, output_dir=output_dir, years=years, overwrite=overwrite)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=log_dir)
