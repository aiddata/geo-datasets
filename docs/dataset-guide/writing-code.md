# Writing Some Code

To start, create a new directory and start editing a new file, e.g. `download.py`.
Start by adding comments explaining the steps you want your script to take.
For example:

```python title="download.py"
url = "http://wm.edu"

# download HTML of URL using requests

# find every link to an image in that HTML

# download each of those images to a folder
```

At this point, you already have a template for your work, and can take your project one step at a time.
You can now look into how to download a URL, and see if you can turn the first comment into code.

## Test As You Go

Each time you add code to your program, save it and then try running it to see if it runs as expected.
If Python raised an exception (prints out an error message), it's important to figure out why that is happening and resolve it before continuing to write code.
In this way, you can iteratively develop your script with confidence that the parts you've already written will continue to work.

!!! tip

    Adding `print()` statements is a great way to debug your script.
    For example, after creating a variable you can print it to see what it is storing.
    Check out the [tips page](../tips) for more advanced debugging techniques.
