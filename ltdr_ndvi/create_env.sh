#!/bin/bash

conda env create -f environment.yml

# comment out if not running in parallel using mpi
pip install mpi4py