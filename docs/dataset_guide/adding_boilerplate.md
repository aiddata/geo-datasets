# Adding Boilerplate

There is some boilerplate code we add to the bottom of all dataset scripts (after the Dataset definition) to set them up for deployment.

## Flow Definition

We add a Prefect flow definition that allows this dataset to be deployed to Prefect.
Here is the code, with placeholder names for the `Dataset` and `BaseDatasetConfiguration`:

```python
try:
    from prefect import flow
except:
    pass
else:

    @flow
    def name_of_dataset(config: DatasetConfigurationName):
        DatasetClassName(config).run(config.run)
```

## Main Function

We use the common `if __name__ == "__main__"` syntax to instantiate the dataset and run it if the `main.py` script is run directly.
Here is the template, to applied similarly to the one above:

```python
if __name__ == "__main__":
    config = get_config(DatasetConfigurationName)
    DatasetClassName(config).run(config.run)
```
