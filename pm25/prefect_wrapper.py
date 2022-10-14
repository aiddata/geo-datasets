from prefect import task
from utils import convert_file

@task
def convert_wrapper(input_file, output_file):
    return convert_file(input_file, output_file)
    
