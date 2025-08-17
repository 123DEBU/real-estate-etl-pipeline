#!/bin/bash
cd ~/Desktop/airflow-tutorial/
source ~/unset_jupyter.sh
unset PYTHONPATH
echo $PYTHONPATH
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
conda init bash
conda activate airflow-tutorial
airflow scheduler


