#!/bin/tcsh
#PBS -N ax-gadm
#PBS -l nodes=3:c18c:ppn=4
#PBS -l walltime=24:00:00
#PBS -j oe

set f = ${HOME}/active/develop/asdf-datasets/data_prep/gadm/gadm_ingest.py

mpirun --mca mpi_warn_on_fork 0 --map-by node -np 12 python-mpi $f develop 2.8 parallel update true
