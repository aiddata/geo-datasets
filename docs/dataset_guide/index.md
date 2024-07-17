# Writing Pipelines

!!! info

    This guide is written with a general audience in mind, including folks who are new to data pipelines or programming in Python.

GeoQuery is, to end users, a website that allows them to download prepared geospatial data.
On the backend side, our job is to make sure this data is available (and current).
We collect data from various sources, converting it to a standard format, and ingest it into the system that runs the website.


# Overview

It's important to make a plan before writing a data pipeline.
Where is the data coming from, and where does it need to go?
Here is an overview of what this looks like:

1. Read configuration
    - Where to store downloaded and processed data?
    - What years of data should be downloaded?
    - If there is already a downloaded file, should it be overwritten?

2. Download data
    - If authentication is required in order to download, we do so
    - Make sure every file gets downloaded correctly, retrying if necessary
    - If the downloaded data is compressed (e.g. a .zip file), decompress it

3. Process data
    - Read original file format
    - If necessary, apply any filters or quality assurance logic to the data
    - Write it out in a standard format (COG, more on that later)

Loading this data into the website infrastucture is done separately so that we can manually check everything before publishing it.
