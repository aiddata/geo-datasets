# Tips & Tricks

Below are some nice things to know as you're getting started.

## pathlib

[pathlib](https://docs.python.org/3.11/library/pathlib.html) is a module built in to Python 3.4+ that makes working with filepaths easier.

Without something like pathlib, it can be tempting to create file path strings using string concatenation, like this:

```python
dst_path = "/path/to/dst/"

year_path = "/path/to/dst/2014/median.tif"
```

While this works well enough, creating new variables from scratch for many file increases the risk of spelling errors, makes paths difficult to edit en masse, and requires [repeating yourself](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) over and over.

Here is an example of pathlib accomplishing the same thing:
```python
from pathlib import Path

dst_dir = Path(/path/to/dst)

# you can use slashes to combine parts of a path together
# now year_dir is a Path object pointing to /path/to/dst/2014/median.tif
year_path = dst_dir / "2014" / "median.tif"

# you can use Path.as_posix() to get a string from the path
assert isinstance(year_path.as_posix(), str)
```

Here, if someone were to edit `dst_dir` on the third line to point somewhere else, it would automatically update `year_path` and any other paths that reference it.
Another nifty feature is that `Path` objects provide functions for quick analysis:

```python
from pathlib import Path

example_path = Path("/path/to/dst/hello_world.txt")

example_path.parent # (1)!
example_path.exists() # (2)!
example_path.name # (3)!
example_path.stem # (4)!
```

1. This attribute is also a `Path` object, pointing to the /path/to/dst directory.
2. This returns a boolean representing whether or not this path actually exists on your filesystem.
3. This returns the string `"hello_world.txt"`
4. This returns the string `"hello_world"`

To see a full list of `pathlib` features, checkout out its documentation [here](https://docs.python.org/3/library/pathlib.html).

## The Python Debugger

An underappreciated feature of Python is the [Python Debugger](https://docs.python.org/3/library/pdb.html), a tool that allows you to stop execution of a Python program wherever you like and inspect it line-by-line.
It can be invoked by inserting `#!python breakpoint()` anywhere in your script, like this:

```python
str_var = "Hello, World!"
breakpoint()
```

When executing this code block, Python will set the `str_var` variable and then stop execution, dropping you into an interactive debugging tool.
There are [many commands](https://docs.python.org/3/library/pdb.html#debugger-commands) you can use in this tool, or if you enter valid Python code it will execute normally.
The `interact` command will switch you to a normal Python interpreter, preserving variables and other elements from the current scope.
