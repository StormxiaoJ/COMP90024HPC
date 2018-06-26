#!/bin/bash
#SBATCH -p physical
#SBATCH --output=1n8c.txt
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=00:15:00

module load Python/3.4.3-goolf-2015a 
mpiexec python cloudpal.py bigInstagram.json
