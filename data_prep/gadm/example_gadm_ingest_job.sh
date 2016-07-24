#!/bin/tcsh
#PBS -N ax-gadm
#PBS -l nodes=3:c18c:ppn=
#PBS -l walltime=24:00:00
#PBS -j oe

mpirun --mca mpi_warn_on_fork 0 --map-by node -np 12 python-mpi gadm_ingest.py develop 2.8 parallel update true
