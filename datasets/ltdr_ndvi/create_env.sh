#!/bin/bash

conda env create -f environment.yml

# comment out if not running in parallel using mpi
conda activate ltdr_ndvi
pip install mpi4py
conda deactivate