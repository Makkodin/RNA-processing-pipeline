import os
from glob import glob
import shutil
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def process_flowcell_by_organism(
    table_path,
    flowcell_name,
    raw_fastq_root='0.Data',
    output_data_root='1.FASTQ',
    delete_original=False
):
    """
    Reads a flowcell from the FASTQ directory and moves FASTQ files to organism-specific subfolders in 1.FASTQ.
    If all files are successfully moved and delete_original=True, the original folder in 0.Data is deleted.

    Parameters:
        table_path (str): Path to the CSV metadata file.
        flowcell_name (str): Name of the flowcell to process.
        raw_fastq_root (str): Root folder containing original FASTQs (default: '0.Data').
        output_data_root (str): Root folder for processed data (default: '1.FASTQ').
        delete_original (bool): Whether to delete the original flowcell folder after moving files (default: False).
    """

    # Загружаем таблицу
    df = pd.read_csv(table_path, low_memory=False)
    group   = df[df['Flowcell'] == flowcell_name]
    group   = group[group['Desct_TYPE'].isin(['RNA_totalRNA', 'RNA_PolyA', 'RNA_BULK','RNA_totalRNA_XP'])]
    group['Descr_ORG']   =   group['Descr_ORG'].replace({'prototype':'human'})

    if group.empty:
        raise ValueError(f"❌[Parse] Flowcell '{flowcell_name}' not found in the metadata.")

    original_fastq_path     =   os.path.join(raw_fastq_root, flowcell_name)
    moved_fastq             =   glob(f"{output_data_root}/*/{flowcell_name}")
    moved_files_count       =   0
    total_file_count        =   0
    already_ready_to_move   =   False
        # Путь к исходным FASTQ 
    if not     moved_fastq:
        if not os.path.isdir(original_fastq_path):
            raise ValueError(f"❌[Parse] FASTQ directory not found: {original_fastq_path}")

        # Определяем организмы
        organisms = group['Descr_ORG'].unique()
        logging.info(f"🕒[Parse] Processing flowcell '{flowcell_name}' with organisms: {', '.join(organisms)}")

        # Считаем общее количество FASTQ-файлов
        total_files = glob(os.path.join(original_fastq_path, "*_S*.fastq.gz"))
        total_files = [x for x in total_files if 'Undetermined' not in x]
        total_file_count = len(total_files)

        if total_file_count == 0:
            logging.warning(f"❌[Parse] No FASTQ files found in {original_fastq_path}. Skipping.")
            return

        # Обрабатываем по организму
        for org in organisms:
            org_dir = os.path.join(output_data_root, org, flowcell_name)
            os.makedirs(org_dir, exist_ok=True)

            samples_for_org = group[group['Descr_ORG'] == org]['Sample_ID'].unique()

            for sample_id in samples_for_org:
                pattern = os.path.join(original_fastq_path, f"{sample_id}_S*.fastq.gz")
                for src_file in glob(pattern):
                    dest_file = os.path.join(org_dir, os.path.basename(src_file))
                    try:
                        shutil.move(src_file, dest_file)
                        moved_files_count += 1
                        #logging.info(f"Moved: {src_file} → {dest_file}")
                    except Exception as e:
                        logging.error(f"❌[Parse] Error moving {src_file}: {e}")
    else:
        print(f"✅[Parse] Flowcell already parse: {moved_fastq}")
        already_ready_to_move = True

    # Удаление исходной директории
    if delete_original:
        if moved_files_count == total_file_count or already_ready_to_move == True:
            try:
                shutil.rmtree(original_fastq_path)
                logging.info(f"✅[Parse] Original FASTQ folder deleted: {original_fastq_path}")
            except OSError:
                logging.warning(f"❌[Parse] Directory not empty, could not delete: {original_fastq_path}")
            except Exception as e:
                logging.error(f"❌[Parse] Error deleting folder {original_fastq_path}: {e}")
        else:
            logging.warning('\n',
                f"❌[Parse] Not all files were moved.\n"
                f"❌[Parse] Moved {moved_files_count} out of {total_file_count}.\n"
                f"❌[Parse] Original folder will NOT be deleted: {original_fastq_path}"
            )