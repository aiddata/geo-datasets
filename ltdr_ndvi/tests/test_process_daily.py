import pytest
import numpy as np
from prepare_daily import create_mask

@pytest.fixture
def example_qa_array():
    return np.array([[*range(-32768, 32768)] for y in range(5)]).astype(np.int16)

@pytest.fixture
def example_mask_vals():
    return [15, 9, 8, 1]

def test_create_mask(example_qa_array, example_mask_vals):
    
    output_mask = create_mask(example_qa_array, example_mask_vals)

    # create_mask preserves array shape
    assert output_mask.shape == (5, 65536)

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
