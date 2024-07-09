import os
import pytest
import pandas as pd
import numpy as np
import random
import string
from datetime import timedelta, date, datetime
from prepare_daily import create_mask, build_data_list

# TODO: test to see if the basepaths set in prepare_daily.py are valid
# TODO: test to see if the filter_options are valid

@pytest.fixture
def example_filter_options():
    # TODO: test different filter options

    filter_options = {
        'use_sensor_accept': False,
        'sensor_accept': [],
        'use_sensor_deny': False,
        'sensor_deny': [],
        'use_year_accept': False,
        'year_accept': ['1987'],
        'use_year_deny': True,
        'year_deny': ['2019']
    }
    return filter_options

@pytest.fixture
def example_input_files(tmp_path):

    # TODO: try sending a blank path string for input_base and output_base.

    #base_folder_path = os.path.abspath(tmp_path)

    # Create an example input folder in tmp_path
    input_folder = tmp_path / "input_base"
    input_folder.mkdir()
    input_folder_path = os.path.abspath(input_folder)

    # Create an example output folder in tmp_path
    output_folder = tmp_path / "output_base" / "daily"

    # Create example data (5 random filenames in nested folders)
    letters = string.ascii_letters
    input_filenames_list = []
    expected_built_array = []
    cur_input_folder = input_folder
    for x in range(5):
        # Nest this next file in a new folder
        cur_input_folder = cur_input_folder / ''.join(random.choice(letters) for i in range(10))
        cur_input_folder.mkdir()

        # Create example filename substrings
        product_code = ''.join(random.choice(letters) for i in range(7))
        # Doesn't test fancier date exceptions such as leap days, invalid years

        # YYYY-MM-DD
        daterange_start = date.fromisoformat('1981-01-01')
        daterange_days = (date.today() - daterange_start).days
        daterange_random = random.randrange(daterange_days)
        random_date = daterange_start + timedelta(days=daterange_random)

        year_string = str(random.randrange(1981, 2020))
        month_string = random_date.strftime('%m')
        day_of_month = random_date.strftime('%d')
        day_of_year = random_date.strftime('%j')

        date_string = "A" + year_string + day_of_year
        sensor_code = random.choice(letters) + ''.join(random.choice(string.digits) for i in range(2))
        misc_string = ''.join(random.choice(string.digits) for i in range(3))
        # Not passing a valid processed_date.
        processed_date = ''.join(random.choice(string.digits) for i in range(13))

        # Concatenate substrings to create example .hdf filename
        input_filename = product_code + "." + date_string + "." + sensor_code + "." + misc_string + "." + processed_date + ".hdf"

        # Write to the file to create it
        input_file = cur_input_folder / input_filename
        # TODO: test example file contents instead of dummy string
        input_file.write_text("testing1,2,3")
        input_file_path = os.path.abspath(input_file)

        # Append this file's info to arrays that this function will return
        input_filenames_list.append(input_filename)

        output_filename = "avhrr_ndvi_v5_" + sensor_code + "_" + year_string + "_" + day_of_year + ".tif"
        output_file_path = os.path.abspath(output_folder / output_filename)
        expected_built_array.append([
            input_file_path,
            sensor_code,
            year_string,
            month_string,
            day_of_year,
            (year_string + "_" + month_string),
            (year_string + "_" + day_of_year),
            output_file_path])

    output_folder = tmp_path / "output_base"
    output_folder_path = os.path.abspath(output_folder)

    expected_built_df = pd.DataFrame(expected_built_array, columns = [
            "input_path",
            "sensor",
            "year",
            "month",
            "day",
            "year_month",
            "year_day",
            "output_path"])

    expected_built_df = expected_built_df.sort_values(by=["input_path"])

    return input_folder_path, output_folder_path, input_filenames_list, expected_built_df

# Create example inputs for test_create_mask
@pytest.fixture
def example_qa_array():
    return np.array([[*range(-32768, 32768)] for y in range(3)]).astype(np.int16)

# Create example mask bits for test_create_mask
# Note that these are the same mask bits used in the original program.
@pytest.fixture
def example_mask_vals():
    return [15, 9, 8, 1]

def test_build_data_list(example_input_files, example_filter_options):

    input_folder_path, output_folder_path, input_filenames_list, expected_built_df = example_input_files
    day_df = build_data_list(input_folder_path, output_folder_path, example_filter_options)

    assert day_df.equals(expected_built_df) 

# This function is meant to test if the create_mask() function is a worthy
# rewrite of the previous code
def test_create_mask(example_qa_array, example_mask_vals):
    
    output_mask = create_mask(example_qa_array, example_mask_vals)

    # create_mask preserves array shape
    assert output_mask.shape == (3, 65536)

    # Test create_mask against old method of masking
    # Set inputs
    qa_array = example_qa_array
    qa_mask_vals = example_mask_vals

    # ORIGINAL CODE - LINES UNEDITED

    binary_repr_v = np.vectorize(np.binary_repr)

    flag = lambda i: bool(int(max(np.array(list(i))[qa_mask_vals])))
    flag_v = np.vectorize(flag)

    qa_bin_array = binary_repr_v(qa_array, width=16)

    qa_mask_vals = [abs(x - 15) for x in qa_mask_vals]

    qa_mask = flag_v(qa_bin_array)

    # END CODE BLOCK

    # create_mask gives the same output as the old operations
    assert np.array_equal(qa_mask, output_mask)
