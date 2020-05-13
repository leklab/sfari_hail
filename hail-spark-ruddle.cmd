#!/bin/bash
#SBATCH --nodes=8
#SBATCH --mem-per-cpu=4G
#SBATCH --cpus-per-task=16
#SBATCH --ntasks-per-node=1
#SBATCH --output=sparkjob-%j.out

#Multi-node script taken from Stanford website https://www.sherlock.stanford.edu/docs/software/using/spark/

## --------------------------------------
## 0. Preparation
## --------------------------------------

#load the Spark module
#module load spark

export SPARK_HOME=/gpfs/ycga/project/lek/shared/tools/spark-2.4.3-bin-hadoop2.7
export PATH=$PATH:/gpfs/ycga/project/lek/shared/tools/spark-2.4.3-bin-hadoop2.7/bin
export PATH=$PATH:/gpfs/ycga/project/lek/shared/tools/spark-2.4.3-bin-hadoop2.7/sbin

module load GCC/5.4.0-2.26

#hail currently runs in conda environment that contains Python 3
#module load miniconda/4.6.14
#. "/ycga-gpfs/apps/hpc/software/miniconda/4.6.14/etc/profile.d/conda.sh"
#conda activate hail

# identify the Spark cluster with the Slurm jobid
export SPARK_IDENT_STRING=$SLURM_JOBID

# prepare directories
export SPARK_WORKER_DIR=${SPARK_WORKER_DIR:-$HOME/.spark/worker}
export SPARK_LOG_DIR=${SPARK_LOG_DIR:-$HOME/.spark/logs}
#export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS:-/tmp/spark}
export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS:-/tmp/${USER}/spark}
mkdir -p $SPARK_LOG_DIR $SPARK_WORKER_DIR

## --------------------------------------
## 1. Start the Spark cluster master
## --------------------------------------

start-master.sh
sleep 1
MASTER_URL=$(grep -Po '(?=spark://).*' \
             $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.*master*.out)

## --------------------------------------
## 2. Start the Spark cluster workers
## --------------------------------------

# get the resource details from the Slurm job
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK:-1}
export SPARK_MEM=$(( ${SLURM_MEM_PER_CPU:-4096} * ${SLURM_CPUS_PER_TASK:-1} ))M
export SPARK_DAEMON_MEMORY=$SPARK_MEM
export SPARK_WORKER_MEMORY=$SPARK_MEM
export SPARK_EXECUTOR_MEMORY=$SPARK_MEM

# start the workers on each node allocated to the tjob
export SPARK_NO_DAEMONIZE=1
srun  --output=$SPARK_LOG_DIR/spark-%j-workers.out --label \
      start-slave.sh ${MASTER_URL} &

## --------------------------------------
## 3. Submit a task to the Spark cluster
## --------------------------------------

#spark-submit --master ${MASTER_URL} \
#             --total-executor-cores $((SLURM_NTASKS * SLURM_CPUS_PER_TASK)) \
#             $SPARK_HOME/examples/src/main/python/pi.py 100

python submit.py --master ${MASTER_URL} hail_scripts/hail_annotate_pipeline.py \
				--spark-home $SPARK_HOME --num-executors $((SLURM_NTASKS * SLURM_CPUS_PER_TASK)) \
				--driver-memory 16G --executor-memory 8G \
				-i ~/scratch60/SFARI/20190729_Merged-GATK4+GLnexus_cleaned_Dec62019_chr_1_monoa_rem.vcf.bgz \
				-m ~/project/SFARI/SPARK_meta.tsv

## --------------------------------------
## 4. Clean up
## --------------------------------------

# stop the workers
scancel ${SLURM_JOBID}.0

# stop the master
stop-master.sh

