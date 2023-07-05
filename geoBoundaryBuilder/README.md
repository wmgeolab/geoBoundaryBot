To setup the environment for the builds:
1) Install the geoBoundariesBuild.yml using conda.  This will give you the base geopandas environment.
2) Activate the conda environment, and make sure the names match your new environment in "buildRunVortex" 
3) module load mvapich2-ib
4) module load intel/2018
5) pip install mpi4py==3.1.4