# Overview of the `Dataset` class

The idea behind the `Dataset` class is that it represents the complete logic of a dataset import, 

["provide a means of bundling data and functionality together."](https://docs.python.org/3/tutorial/classes.html)


## `main()`

When a dataset is run (see "Running a dataset" below), `main()` gets called.

`main()` defines the flow of the dataset run, describing the order of each set of tasks.

Usually `main()` is relatively small, calling `self.run_tasks()` for the download function, handling the results, and then passing those into a process task run.
When first reading a `Dataset` file, this is a good place to start in order to understand the steps involved with running a dataset.


## `run_tasks()`

...

## `tmp_to_dst_file()`

...
