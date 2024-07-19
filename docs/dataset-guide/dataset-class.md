# Overview of the `Dataset` class

The idea behind the `Dataset` class is that it represents the complete logic of a dataset import, providing ["a means of bundling data and functionality together"](https://docs.python.org/3/tutorial/classes.html).
Once you have determined what you want your script to accomplish, this class provides a framework that:

- Organizes groups of tasks into "task runs," standardizing their outputs and logging their progress,
- Provides convenience functions to help manage common tasks in safer ways, and
- Takes care of running the pipeline on our backend infrastructure.

The `Dataset` class is provided by a Python package called data_manager, stored in the `/data_manager` directory in the geo-datasets repository.
By updating the data_manager package, we can update the behavior of all pipelines at once.
Each dataset (in `/datasets`) can choose to use any version of data_manager using a configuration parameter (more on that later).

## The `Dataset` Class

### Required Functions

#### `main()`

When a `Dataset` is run, `Dataset.main()` gets called.
`main()` defines the game plan for a dataset run, describing the order of each set of tasks.
To do this, `main()` contains function calls wrapped with `self.run_tasks()` to manage groups of tasks.

### Provided Functions

#### `run_tasks()`

...

#### `tmp_to_dst_file()`

...

### Adding Your Own Functions

When writing a `Dataset`, it will be necessary to add your own functions to power it.
For example, most pipelines will include functions to download units of data.
This is illustrated in the template code below.

## The `BaseDatasetConfiguration` Model

!!! info

    In pydantic lingo, a "model" is a class that inherits `pydantic.BaseModel` and includes internal type-checking logic.
    Check out the pydantic documentation for more information.

`BaseDatasetConfiguration` is a pydantic model that represents the configuration parameters for running a dataset.
As well as defining a class that inherits `Dataset`, you should also define a configuration class that inherits `BaseDatasetConfiguration`

### The `run` Parameter

It comes with one built-in parameter out-of-the-box, called `run`.
`run` defines the options for how the computer should run the dataset, such as if the tasks should be ran sequentially or in parallel.
The config file (see below) can override any of the default run parameters in the `[run]` table.


## Main Script Template

```python title="main.py"
from pathlib import Path

from data_manager import BaseDatasetConfiguration, Dataset, get_config# (1)!


class ExampleDatasetConfiguration(BaseDatasetConfiguration):# (2)!
    raw_dir: str
    output_dir: str
    years: List[int]# (3)!
    overwrite_download: bool
    overwrite_processing: bool


class ExampleDataset(Dataset):# (4)!
    name = "Official Name of Example Dataset"# (5)!

    def __init__(self, config: ESALandcoverConfiguration):# (6)!
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)# (7)!
        self.years = config.years
        self.overwrite_download = config.overwrite_download# (8)!
        self.overwrite_processing = config.overwrite_processing

    def download(self, year):# (9)!
        logger = self.get_logger()
        # Logic to download a year's worth of data
        return output_file_path

    def process(self, input_path, output_path):
        logger = self.get_logger()

        if self.overwrite_download and not self.overwrite_processing:
            logger.warning("Overwrite download set but not overwrite processing.")# (10)!

        if output_path.exists() and not self.overwrite_processing:
            logger.info(f"Processed layer exists: {input_path}")

        else:
            logger.info(f"Processing: {input_path}")

            tmp_input_path = self.process_dir / Path(input_path).name
        return

    def main(self):
        logger = self.get_logger()

        os.makedirs(self.raw_dir / "compressed", exist_ok=True)
        os.makedirs(self.raw_dir / "uncompressed", exist_ok=True)

        # Download data
        logger.info("Running data download")
        download = self.run_tasks(self.download, [[y] for y in self.years])
        self.log_run(download)

        os.makedirs(self.output_dir, exist_ok=True)

        # Process data
        logger.info("Running processing")
        process_inputs = zip(
            download.results(),
            [self.output_dir / f"esa_lc_{year}.tif" for year in self.years],
        )
        process = self.run_tasks(self.process, process_inputs)
        self.log_run(process)

# ---- BEGIN BOILERPLATE ----(11)
try:
    from prefect import flow
except:
    pass
else:
    @flow
    def name_of_dataset(config: DatasetConfigurationName):
        DatasetClassName(config).run(config.run)

if __name__ == "__main__":
    config = get_config(DatasetConfigurationName)
    DatasetClassName(config).run(config.run)
```

1. This import is explained in full in the [Adding Boilerplate](../adding-boilerplate) section.
2. This is the configuration pydantic model, inherited from `BaseDatasetConfiguration`. See [configuration](configuration) for more information.
3. Since pydantic type checks when data is loaded into a model, this type hint enforces the concent of the config file `config.toml`.
   If the type is `#!python List[int]`, the TOML representation of this parameter will have to look something like:
   ```toml
   years = [ 2001, 2002, 2003 ]
   ```
4. Here is the main `Dataset` definition.
   Note that each of its attributes and methods are indented below.
   Also, the Python community [has decided](https://peps.python.org/pep-0008/#class-names) to name classes using the CapWords convention.
5. This `#!python str` attribute of the `Dataset` class should be set to the full proper name of the dataset, for convenient reference.
   In the Prefect UI, deployed pipelines will be labeled with this name.
6. The `__init__()` function is called when a class is first instantiated.
   This function sets all of the variables with `Dataset` (stored as attributes of `self`) for future reference by the other methods within `Dataset`.
7. `pathlib.Path` makes working with file paths so much nicer.
   More on that [here](../tips#pathlib).
8. All these "`self.XXX = config.XXX`" lines could be replaced with a single `self.config = config` statement.
   Then, other methods could reference `self.config.overwrite_download`, for example.
   Your call as to what feels cleaner / more ergonomic.
9. Here is the first custom method in this example.
   When this `Dataset` class is run, the `main()` method will call this `download()` method for each year it wants to download.
10. Here is a nice example of the `logger` in use.
    As long as you add the line `logger = self.get_logger()` at the top of any `Dataset` method, you can call it to automatically log pipeline events.
    `logger` supports the levels `debug`, `info`, `warning`, `error`, and `critical`.
11. Explained in detail in the [Adding Boilerplate](../adding-boilerplate) section.

## Configuration

In addition to `main.py`, we store configuration values in a separate [TOML](https://toml.io/en/) file, `config.toml`.

### How the Config File is Loaded

...


### Template Config File

```toml title="config.toml"
# top-level key/value pairs load into dataset configuration(1)
raw_dir = "/sciclone/aiddata10/REU/geo/raw/esa_landcover"

years = [ 2018, 2019, 2020 ]

overwrite_download = false

api_key = "f6d4343e-0639-45e1-b865-84bae3cce4ee"


[run]# (2)!
max_workers = 4
log_dir = "/sciclone/aiddata10/REU/geo/raw/example_dataset/logs"# (3)


[repo]# (4)!
url = "https://github.com/aiddata/geo-datasets.git"
branch = "master"
directory = "datasets/example_dataset"# (5)!


[deploy]# (6)!
deployment_name = "example_dataset"
image_tag = "05dea6e"# (7)!
version = 1
flow_file_name = "main"
flow_name = "example_dataset"
work_pool = "geodata-pool"
data_manager_version = "0.4.0"# (8)!
```

1. As this comment implies, the top-level key/value pairs (those not within a [table] as seen below) are loaded into a `BaseDatasetConfiguration` model as defined in `main.py`.
2. These...
3. This is the one required parameter in the `run` table.
   `log_dir` instructs the `Dataset` where to save log files for each run.
4. The `repo` table instructs the deployment where to find the dataset once it's been pushed to the geo-datasets repository on GitHub.
   This table should generally be left as-is, replacing "example_dataset" with the name of your dataset as appropriate.
5. This refers to the path to the dataset directory relative to the root of the repository.
6. The `deploy` table provides the deployment script with settings and metadata for the Prefect deployment.
7. The OCI image tag for the container to run this deployment in.
   See [the deployment guide](/deployment-guide/build-container) for more information.
8. The data_manager package is versioned using git tags, pushed to the geo-datasets repository on GitHub.
   This string specifies which tag to pull from GitHub and install when the container spins up.
