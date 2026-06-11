"""
This package provides a framework for running ingest pipelines for GeoQuery, consisting of base classes meant to be inherited by ingest scripts.
"""

from .configuration import BaseDatasetConfiguration, get_config
from .dataset import Dataset

__version__ = "0.4.6"
