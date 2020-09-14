#! /bin/bash

###################
#
# Launching shell script for NIAID CSI batch processing of WES data
#
###################
module load snakemake/5.7.4
module load python/3.7

##
## Location of snakemake
##
DIR="$( cd -P "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
echo "Running script from ${DIR}"

##
## Test commandline arguments
##
if [ $# -ne 3 ]; then
    echo " " 
    echo "Requires a raw data directory, processing type: align, varcall, or qc and run argument: gris, npr, or process"
    echo " " 
    exit
fi

if [ $3 != "gris" ] && [ $3 != "npr" ] && [ $3 != "process" ] ; then
    echo " " 
    echo "Invalid commandline option: $3"
    echo "Valid commandline options include: gris, npr, or process"
    echo " " 
    exit
fi

##
## Get batch and batch number
##
batchdir=`pwd`
batch=`echo $batchdir | sed -e 's/^.*\///' `
echo "BATCH: $batch"
#batchnumber=`echo $batch | sed -e 's/BATCH//' -e 's/^0//' `
#echo "Processing Batch $batchnumber"

##
## Find the raw directory
##
#raw_root="/data/GRIS_NCBR/rawdata"
raw_dir=$1

if ! test -d "rawdata"; then
    if test -d $raw_dir; then
        echo "Linking rawdata subdirectory to $raw_dir"
        ln -s $raw_dir rawdata
    else
        echo "Unable to locate raw data directory $raw_dir"
        echo "Exiting"
        exit
    fi
else
    echo "input directory rawdata already exists"
fi

##
## Make the new output directories
##
for i in BAM VCF BATCH_QC HLA inbreeding
do
    if ! test -d $i; then
        echo "Creating output directory: $i"
        mkdir -p $i
    else
        echo "output directory $i already exists"
    fi
done
# and be sure this directory and all subdirectories are writable
chmod -fR g+rwx .

SCRIPT=`realpath $0`
SCRIPTPATH=`dirname $SCRIPT`

echo $SCRIPT
echo $SCRIPTPATH

##
## Run csi_to_gris.py
##
#if [ "$3" == "gris" ] 
#then
#    echo "Running csi_to_gris"
#    "$SCRIPTPATH/"csi_to_gris_hg38.py -b $batchnumber
#    exit
#fi

if ! test -e "masterkey.txt"; then
  echo 'Names' > key1.txt
  echo 'IDs' > key2.txt
  ls rawdata/*L001_R1_001.fastq.gz | cut -d'/' -f2 | cut -d'.' -f1 >> key1.txt
  sed -e s/_L001_R1_001//g -i key1.txt
  ls rawdata/*L001_R1_001.fastq.gz | cut -d'/' -f2 | cut -d'.' -f1 | cut -d'_' -f1 >> key2.txt
  paste key1.txt key2.txt > masterkey.txt
  mkdir -p snakejobs
fi
#mkdir -p BATCH_QC

##
## Run snakemake
##
echo "Run snakemake"

#CLUSTER_OPTS="qsub -e snakejobs -o snakejobs -wd $batchdir"
CLUSTER_OPTS="sbatch --gres {cluster.gres} --qos=cv19 --cpus-per-task {cluster.threads} -p {cluster.partition} -t {cluster.time} --mem {cluster.mem} --job-name={params.rname} -e snakejobs/slurm-%j_{params.rname}.out -o snakejobs/slurm-%j_{params.rname}.out --chdir=$batchdir"
#CLUSTER_OPTS="qsub -e snakejobs -o snakejobs -pe threaded {cluster.threads} -l {cluster.partition} -l h_vmem={cluster.vmem} -l mem_free={cluster.mem} -wd $batchdir"
#CLUSTER_OPTS="qsub -e snakejobs -o snakejobs -pe threaded {cluster.threads} -l {cluster.partition} -cwd"
#CLUSTER_OPTS_HIMEM="qsub -e snakejobs_himem -o snakejobs_himem -pe threaded 8 -l himem -l h_vmem=32G -l virtual_free=32G -wd $batchdir"
if [ "$2" == "align" ]
then
    RUNFILE="ncbr_wgs_rapid_$4_align.snakemake"
fi

if [ "$2" == "varcall" ]
then
    RUNFILE="ncbr_wgs_rapid_$4_varcall.snakemake"
fi

if [ "$2" == "qc" ]
then
    RUNFILE="ncbr_wgs_rapid_$4_qc.snakemake"
fi

if [ "$3" == "npr" ]
then
    snakemake -n --snakefile ${DIR}/"$RUNFILE"
fi

if [ "$3" == "process" ]
then
    snakemake --stats snakemake.stats --restart-times 1 --rerun-incomplete -j 500 --cluster "$CLUSTER_OPTS" --cluster-config NCBR_wgs_pipeline/ncbr_wgs_cluster.json --keep-going --snakefile ${DIR}/"$RUNFILE" > wgs_batch_processing.log 2>&1 &
    #snakemake --stats snakemake.stats --restart-times 1 --rerun-incomplete -j 100  --cluster "$CLUSTER_OPTS" --cluster-config ${DIR}/cluster.json --keep-going --snakefile ${DIR}/csi_batch_processing.snakemake > csi_batch_processing.log 2>&1 &
fi