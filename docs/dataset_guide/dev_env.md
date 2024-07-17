# Setting Up Your Environment

Before developing a dataset pipeline, you'll need a development environment with the appropriate packages installed.

!!! note

    You will need to use a command-line interface to work with conda and other Python-related tools.
    A great resource for getting comfortable with the command-line is the [MIT Missing Semester](https://missing.csail.mit.edu/) course, which is available for free online.
    If you use Microsoft Windows, please consider installing Windows Subsystem for Linux.


## Environment Management System

When developing on your local machine, you'll likely need a system for compartmentalizing environments you use for different development projects.
While this setup is entirely up to you, we've found success using [conda](https://docs.conda.io/projects/conda/en/stable/). Another option is [mamba](https://mamba.readthedocs.io/en/latest/) is a faster alternative to conda that is fully compatible.

Whichever tool you choose, follow its installation instructions before proceeding.


## Clone the geo-datasets Repository

1. Make sure git is installed.
2. `cd` to the directory you'd like to clone geo-datasets into.
   This can be `~/Documents`, for example
3. Run `git clone git@github.com:aiddata/geo-datasets.git`.
4. `cd` into `geo-datasets`.


## Install Dependencies

!!! note

    This section assumes you are using conda (or mamba).
    If you are using some other environment management system, you'll have to adapt these instructions accordingly.

1. Create an environment for geo-datasets.
   We usually name the environment "geodataXXX", replacing the "XXX" with the version of Python we are currently using.
   At the time of writing, that was 3.11:
   ```
   conda create -n geodata311 python=3.11
   ```
2. Activate your new environment
   ```
   conda activate geodata311
   ```
3. Change directory to the `kubernetes/containers/job-runner` subdirectory of the geo-datasets repository
   ```
   cd geo-datasets/kubernetes/containers/job-runner
   ```
4. Install Python packages used by the latest job runner
   ```
   pip install -r requirements.txt
   ```
