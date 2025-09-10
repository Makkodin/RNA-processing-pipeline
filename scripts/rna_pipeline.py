import os
import logging
import sys 
from    glob import glob
import  pandas as pd
import  tqdm

from Run_pipeline.load_ceph_sheet   import  load_airflow_parse
from Run_pipeline.load_data         import  load_fastq
from Run_pipeline.parse_flowcell    import  process_flowcell_by_organism
from Run_pipeline.create_ss         import  generate_samplesheet
from Run_pipeline.nfcore            import  run_nextflow_pipeline
from Run_pipeline.move_to_ceph      import  move_to_ceph_and_remove

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define paths and mapping
#FLOWCELL    =   sys.argv[1]  
WORK_DIR    =   "/mnt/raid0/ofateev/projects/RNASeq_Pipeline"
REF_FOLDER  =   "/mnt/raid0/ofateev/refs"
CEPH_FASTQ  =   "/mnt/cephfs*/FASTQS/uvd*"
CEPH_SAVE   =   "/mnt/cephfs8_rw/functional-genomics/RNASeq_pipeline_RES"

username    =   'ofateev'
password    =   '112358Iop24???'

# Mapping from ORG (folder name) to REF_ORG (reference genome)
org_map = {
    "human":    "GRCh38",
    "mouse":    "MM10",
    "mulatta":  "Mmul10",
    "jacchus":  "mCalJac1",
    "prototype":"GRCh38"
}

def main():

    fastq_raw   =   f"{WORK_DIR}/0.FASTQ"
    fastq_split =   f"{WORK_DIR}/1.Data"

    info_sheet_path = load_airflow_parse(
        info_sheet_ceph8='/mnt/cephfs8_rw/functional-genomics/ofateev/Parse_df/results_parsing.csv',
        info_sheet      =f'{WORK_DIR}/3.Info/results_parsing.csv'
    )
    
    df = pd.read_csv(info_sheet_path,low_memory=False)
    df_RNA = df[df['Desct_TYPE'].isin(['RNA_totalRNA', 'RNA_PolyA', 'RNA_BULK', 'RNA_totalRNA_XP'])]

    for FLOWCELL in sorted(df_RNA['Flowcell'].drop_duplicates().to_list(), reverse=True):
        if len(glob(f'/mnt/cephfs8_rw/functional-genomics/RNASeq_pipeline_RES/*/{FLOWCELL}')) == 0:
            print("\033[91m" + "=" * 53 + "\033[0m")

            fastq_res_folder    =   'Exist 0.FASTQ'
            if len(glob(f"{fastq_split}/*/{FLOWCELL}")) == 0:
                fastq_res_folder = load_fastq(
                        flowcell=FLOWCELL,
                        save_fastq=fastq_raw,
                        load_fastq=CEPH_FASTQ,
                        user=username,
                        password=password,
                    )

            if fastq_res_folder == 'Exist 0.FASTQ':
                move_flowcell   =   process_flowcell_by_organism(
                        table_path          =   f'{WORK_DIR}/3.Info/results_parsing.csv',
                        flowcell_name       =   FLOWCELL,
                        raw_fastq_root      =   fastq_raw,
                        output_data_root    =   fastq_split,
                        delete_original     =   True
                )


            output_data_root            =   os.path.join(fastq_split)
            organisms                   =   [d for d in os.listdir(output_data_root) 
                                            if os.path.isdir(os.path.join(output_data_root, d))]


            if not organisms:
                logging.warning("No organism directories found in output_data_root.")
                return

            found_flowcell = False

            for org in organisms:
                ref_org = org_map.get(org.lower())

                if not ref_org:
                    logging.warning(f"Organism '{org}' not found in org_map. Skipping.")
                    continue

                flowcell_path = os.path.join(output_data_root, org, FLOWCELL)


                if os.path.isdir(flowcell_path):
                    logging.info(f"Found flowcell directory: {flowcell_path}")

                    results_path =  generate_samplesheet(FLOWCELL   =   FLOWCELL,
                                                         WORK_DIR   =   WORK_DIR,
                                                         ORG        =   org
                                                         )

                    report_check = os.path.join(results_path, FLOWCELL, 'results','multiqc', 'hisat2', 'multiqc_report.html')
                    if os.path.exists(report_check) == False:
                        run_nextflow_pipeline(FLOWCELL  =   FLOWCELL, 
                                              ORG       =   org, 
                                              REF_ORG   =   ref_org, 
                                              WORK_DIR  =   WORK_DIR, 
                                              REF_DIR   =   REF_FOLDER)
                        found_flowcell = True
                    else:
                        print(f'{report_check} : exist!')

                    move_to_ceph_and_remove(work_dir   =   WORK_DIR,
                                 ceph_path  =   CEPH_SAVE,
                                 organism   =   org,
                                 flowcell   =   FLOWCELL,)
#       
                else:
                    logging.warning(f"Flowcell directory not found: {flowcell_path}. Skipping.")
            if not found_flowcell:
                logging.error("No flowcell directories found for any organism.")
            print("\033[91m" + "=" * 53 + "\033[0m")

if __name__ == "__main__":
    main()