import sys, os


from prefect.filesystems import GitHub
block_name = "geo-datasets-github"
GitHub.load(block_name).get_directory('global_scripts')

sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'global_scripts'))
sys.path.insert(1, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'malaria_atlas_project'))



from prefect import flow

from main import MalariaAtlasProject


@flow
def malaria_atlas_project(raw_dir, output_dir, years, dataset, overwrite, backend, task_runner, run_parallel, max_workers, log_dir):

    sys.path.append(os.path.join(os.path.realpath(__file__), 'global_scripts'))
    print(sys.path)
    class_instance = MalariaAtlasProject(raw_dir, output_dir, years, dataset, overwrite)

    class_instance.run(backend=backend, task_runner=task_runner, run_parallel=run_parallel, max_workers=max_workers, log_dir=log_dir)
