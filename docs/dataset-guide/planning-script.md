# Planning Your Ingest Script

Most data pipelines we write for GeoQuery have a similar structure.
Without delving into too many implementation specifics (yet!), let's take a look at the common elements of these scripts.

## Download

The download script is responsible for retrieving the data from its source.
Sometimes this is through some sort of API, other times it is through an FTP server, or maybe a file hosting service like [Box](https://www.box.com/).
In many cases the download step takes the longest, since it requires transferring large amounts of data across the internet.
It's also important for us to respect the data providers by keeping our requests to reasonable volume.
For these reasons, it can be a challenge to write an efficient and reliable download script.

### Choosing Packages

The download step is often the most dataset-specific aspect of an ingest pipeline, because the actual process of downloading can vary so much between sources.
For this reason, we often need to find and use existing Python packages that are designed to support a specific protocol, for example [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for downloading from AWS, or [cdsapi](https://cds.climate.copernicus.eu/how-to-api) for downloading from the Climate Data Store.
On the other hand, we try to reduce the number of packages used by the geo-datasets project where possible in order to keep long-term maintenance easier.
When in doubt, get in touch with us and let's chat about what makes most sense for your dataset.

For downloading files directly over HTTP, please use the popular [requests](https://requests.readthedocs.io/en/latest/) package.
requests is very well-documented, and provides a convenient interface for handling HTTP requests.

### Example

Below is an example of some Python code that downloads a website, http://example.com, to /path/to/dst.
This example uses the requests package noted above.
Click on the plus signs to read annotations describing what is going on.

```python
src_url = "http://example.com" # (1)!
dst_path = "/path/to/dst" # (2)!

with requests.get(src_url, stream=True) as src: # (3)!
    src.raise_for_status() # (4)!
    with open(dst_path, "wb") as dst: # (5)!
            dst.write(src.content) # (6)!
```

1. This variable is a string representing the URL to download.
2. This variable is a string representing the filepath to download to.
3. This `with` syntax opens a [context manager](https://realpython.com/python-with-statement/) using the [requests](https://requests.readthedocs.io/en/latest/) library.
   Within the indented block below, the `src` variable is an object that represents the request.
   Context managers are very common (and useful!) in Python!
4. This is a requests-specific function that raises an exception if the HTTP status code indicates an error.
5. Another context manager!
   This time, we are opening a file for writing using the built-in `open` function.
6. This is the meat of this entire script, instructing Python to write the content from our request into the opened file.

Download scripts for different websites can vary dramatically, so it's difficult to show one example that illustrates them all.
That said, a common requirement is to provide an API token when making a request.
Below is some code that builds upon the previous example, adding an API token to the request's HTTP headers:

```python
token = "XXXXX" # (1)!
src_url = "http://example.com"
dst_path = "/path/to/dst"


# dictionary of HTTP headers
headers = {
    "Authorization": f"Bearer {token}",
}

with requests.get(src_url, headers=headers, stream=True) as src: # (2)!
    # raise an exception (fail this task) if HTTP response indicates that an error occured
    src.raise_for_status()
    with open(dst_path, "wb") as dst:
            dst.write(src.content)
```

1. This variable is a string representing some API token for this website.
2. Adding headers to these keyword arguments includes the `headers` dictionary in the HTTP headers of this request.

### Checksums

Especially when we are downloading thousands of images at once, it's possible for a few to get corrupted in the chaos of networking.
In some cases, the data source provides a checksum of the files, so that we can confirm that our copy is correct.
When it's possible, this is great functionality to include in the download step.
If the data has already been downloaded, it's faster to check that it matches a checksum rather than download it all over again.
If the file(s) don't match the checksum, we can write code to automatically download them again before moving on to the processing stage.

If checksums are not available for the data, that is ok.
In this case, it can be helpful to consider what happens when a download job gets interrupted.
For example, we can download files to a temporary location and then move them to their permanent home once the download is complete, preventing the rest of ingest pipeline from trying to use partial files.

## Processing

The primary work a processing task accomplishes is reading the raw data, and writing it into COG files.
To accomplish this, we primarily use the [rasterio](https://github.com/rasterio/rasterio) package.

One key thing to understand about rasterio is that it manages file read and write settings as dictionaries of variables, passed as keyword arguments to the `rasterio.open()` function.
When reading a source file, this dictionary can be accessed at the meta attribute of the opened file object, e.g. `src.meta`.
When writing an output file, this dictionary can be defined as keyword arguments in the `rasterio.open()` command.

```python
import rasterio

with rasterio.open(src_path, "r") as src:
    # src.meta is the profile of the source file
    with rasterio.open(dst_path, "w", **profile) as dst:
        src.write(dst)
```
