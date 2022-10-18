from prefect import task

@task
def task_wrapper(func, *args, **kwargs):
    return func(*args, **kwargs)
    
