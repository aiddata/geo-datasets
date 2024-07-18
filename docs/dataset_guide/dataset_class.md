# Overview of the `Dataset` class

The idea behind the `Dataset` class is that it represents the complete logic of a dataset import, 

["provide a means of bundling data and functionality together."](https://docs.python.org/3/tutorial/classes.html)

## The `Dataset` Class


### `main()`

When a dataset is run (see "Running a dataset" below), `main()` gets called.

`main()` defines the flow of the dataset run, describing the order of each set of tasks.

Usually `main()` is relatively small, calling `self.run_tasks()` for the download function, handling the results, and then passing those into a process task run.
When first reading a `Dataset` file, this is a good place to start in order to understand the steps involved with running a dataset.


### `run_tasks()`

...

### `tmp_to_dst_file()`

...

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

!!! info

    This section contains boilerplate code described in the next section, [adding boilerplate](../adding_boilerplate).

```python title="main.py"
from data_manager import BaseDatasetConfiguration, Dataset, get_config # (1)!


class ExampleDatasetConfiguration(BaseDatasetConfiguration):
    raw_dir: str
    output_dir: str
    years: List[int]
    overwrite_download: bool
    overwrite_processing: bool


class ExampleDataset(Dataset):
    name = "Official Name of Example Dataset"

    def __init__(self, config: ESALandcoverConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.years = config.years
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing

    def download(self, year):
        logger = self.get_logger()
        return output_file_path

    def process(self, input_path, output_path):
        logger = self.get_logger()

        if self.overwrite_download and not self.overwrite_processing:
            logger.warning("Overwrite download set but not overwrite processing.")

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

# ---- BEGIN BOILERPLATE ----
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

1. This import is explained in full in the [Adding Boilerplate](../adding_boilerplate) section.

## Configuration

In addition to `main.py`, we store configuration values in a separate [TOML](https://toml.io/en/) file, `config.toml`.

### How the Config File is Loaded

...


### Template Config File

```toml title="config.toml"
# top-level key/value pairs load into dataset configuration (1)
raw_dir = "/sciclone/aiddata10/REU/geo/raw/esa_landcover"

years = [ 2018, 2019, 2020 ]

overwrite_download = false

api_key = "f6d4343e-0639-45e1-b865-84bae3cce4ee"


[run] # (2)!
max_workers = 4
log_dir = "/sciclone/aiddata10/REU/geo/raw/example_dataset/logs" # (3)


[repo] # (4)!
url = "https://github.com/aiddata/geo-datasets.git"
branch = "master"
directory = "datasets/example_dataset" # (5)!


[deploy] # (6)!
deployment_name = "example_dataset"
image_tag = "05dea6e"
version = 1
flow_file_name = "main"
flow_name = "example_dataset"
work_pool = "geodata-pool"
data_manager_version = "0.4.0"
```

1. As this comment implies, the top-level key/value pairs (those not within a [table] as seen below) are loaded into a `BaseDatasetConfiguration` model as defined in `main.py`.
2. These...
3. This is the one required parameter in the `run` table.
   `log_dir` instructs the `Dataset` where to save log files for each run.
4. The `repo` table instructs the deployment script where to find the dataset once it's been pushed to the geo-datasets repository on GitHub.
   This table should generally be left as-is, replacing "example_dataset" with the name of your dataset as appropriate.
