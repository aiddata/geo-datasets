#!/bin/tcsh
#PBS -N ax-gadm
#PBS -l nodes=10:c18c:ppn=4
#PBS -l walltime=72:00:00
#PBS -j oe

set branch = develop
set f = ${HOME}/active/${branch}/asdf-datasets/data_prep/gadm/gadm_ingest.py

mpirun --mca mpi_warn_on_fork 0 --map-by node -np 40 python-mpi $f $branch 2.8 parallel update 0
