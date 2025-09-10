import os
import subprocess
import logging


def run_nextflow_pipeline(
                         FLOWCELL:str, 
                         ORG:str, 
                         REF_ORG:str,
                         WORK_DIR:str,
                         REF_DIR:str    =   "/mnt/raid0/ofateev/refs"):

    
    RESDIR          = os.path.join(WORK_DIR,        "2.Results",    ORG)
    FLOWCELL_RESDIR = os.path.join(RESDIR,          FLOWCELL)
    WORK            = os.path.join(FLOWCELL_RESDIR, "work")
    D_RESULTS       = os.path.join(FLOWCELL_RESDIR, "results")
    NFCORE_RESULTS  = os.path.join(D_RESULTS)
    LOG_FOLDER      = os.path.join(D_RESULTS,       "process.log")
    CACHE_FOLDER    = os.path.join(FLOWCELL_RESDIR, "cache")
    SHEET_FLOWCELL  = os.path.join(FLOWCELL_RESDIR, "samplesheet.csv")

    # Ensure necessary directories exist
    os.makedirs(RESDIR,         exist_ok=True)
    os.makedirs(FLOWCELL_RESDIR,exist_ok=True)
    os.makedirs(WORK,           exist_ok=True)
    os.makedirs(D_RESULTS,      exist_ok=True)
    os.makedirs(NFCORE_RESULTS, exist_ok=True)
    os.makedirs(CACHE_FOLDER,   exist_ok=True)


    # Build the command to run the Nextflow pipeline
    cmd = f"""
        nextflow -log {LOG_FOLDER} \
            run /mnt/raid0/ofateev/soft/rnaseq_v3.12.0/main.nf \
            -w          {WORK} \
            --input     {SHEET_FLOWCELL} \
            --outdir    {NFCORE_RESULTS}/ \
            -profile    docker \
            --aligner   hisat2 \
            --pseudo_aligner    salmon \
            --fasta             {REF_DIR}/RNA_Pipeline_{REF_ORG}/genome.fa \
            --gtf               {REF_DIR}/RNA_Pipeline_{REF_ORG}/genes.gtf \
            --hisat2_index      {REF_DIR}/RNA_Pipeline_{REF_ORG}/index/hisat2 \
            --salmon_index      {REF_DIR}/RNA_Pipeline_{REF_ORG}/index/salmon \
            --rsem_index        {REF_DIR}/RNA_Pipeline_{REF_ORG}/rsem \
            --gene_bed          {REF_DIR}/RNA_Pipeline_{REF_ORG}/genes.bed \
            --transcript_fasta  {REF_DIR}/RNA_Pipeline_{REF_ORG}/genome.transcripts.fa \
            -resume
    """

    logging.info(f"🕒[nfcore] Starting pipeline for {ORG} ({REF_ORG})")
    logging.debug(f"🕒[nfcore] Command: {cmd}")

    try:
        process = subprocess.run(
            cmd,
            shell=True,
            cwd=CACHE_FOLDER,
            executable="/bin/bash",
            check=True
        )
        logging.info(f"✅[nfcore] Pipeline completed successfully for {ORG}")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌[nfcore] Error running pipeline for {ORG}: {e}")