import os
from glob import glob
import pandas as pd


def generate_samplesheet(FLOWCELL: str, 
                         WORK_DIR: str,
                         ORG: str) -> str:
    """
    Generates a samplesheet for RNA-seq pipeline based on FASTQ files.
    Sample names are taken from the part before '_S' in the filename.

    :param FLOWCELL: Name of the flowcell (e.g., '250515_A00926_0879_BHTN75DMXY')
    :param WORK_DIR: Root working directory (e.g., '/mnt/raid0/ofateev/projects/RNASeq_Pipeline')
    :param ORG: Organism name (e.g., 'human')
    :return: Path to generated samplesheet
    """

    # Пути
    DATA_DIR        = os.path.join(WORK_DIR, "1.Data", ORG, FLOWCELL)
    RESDIR          = os.path.join(WORK_DIR, "2.Results", ORG)
    FLOWCELL_RESDIR = os.path.join(RESDIR, FLOWCELL)
    SHEET_FLOWCELL  = os.path.join(FLOWCELL_RESDIR, "samplesheet.csv")

    # Проверяем, существует ли уже samplesheet
    if os.path.exists(SHEET_FLOWCELL):
        print(f"⚠️[Generated sheet] Samplesheet already exists: {SHEET_FLOWCELL}")
        return RESDIR

    # Ищем FASTQ-файлы
    files = sorted(glob(os.path.join(DATA_DIR, "*_R[12]_*.fastq.gz")))

    # Фильтруем Undetermined
    files = [f for f in files if 'Undetermined' not in f]

    if len(files) == 0:
        raise FileNotFoundError(f"❌[Generated sheet] No FASTQ files found in {DATA_DIR}")

    # Проверяем, что файлов чётное число и пары совпадают
    df_total = pd.DataFrame()
    for i in range(0, len(files), 2):
        if i + 1 >= len(files):
            raise ValueError("❌[Generated sheet] Odd number of FASTQ files found. Are all pairs present?")

        first_fastq = files[i]
        second_fastq = files[i + 1]

        # Получаем имя файла без пути
        filename_r1 = os.path.basename(first_fastq)
        filename_r2 = os.path.basename(second_fastq)

        # Имя образца — всё, что до "_S"
        try:
            sample_name = filename_r1.split('_S')[0]
        except IndexError:
            raise ValueError(f"❌[Generated sheet] Invalid FASTQ filename: {filename_r1} (missing '_S')")

        # Проверяем, что второй файл тоже начинается с этого имени
        if not filename_r2.startswith(sample_name):
            raise ValueError(f"❌[Generated sheet] Mismatched FASTQ pair:\n{first_fastq}\n{second_fastq}")

        strandedness = 'auto'

        df_sample = pd.DataFrame([[sample_name, first_fastq, second_fastq, strandedness]],
                                 columns=['sample', 'fastq_1', 'fastq_2', 'strandedness'])
        df_total = pd.concat([df_total, df_sample], axis=0, ignore_index=True)

    # Сохраняем samplesheet
    os.makedirs(FLOWCELL_RESDIR, exist_ok=True)
    df_total.to_csv(SHEET_FLOWCELL, index=False)
    print(f"✅[Generated sheet] Samplesheet created at: {SHEET_FLOWCELL}")


    return RESDIR