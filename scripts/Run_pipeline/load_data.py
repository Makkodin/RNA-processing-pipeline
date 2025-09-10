import os
import subprocess
from glob import glob
from typing import Optional

def load_fastq(   
        flowcell: str,
        user: str,
        save_fastq: str,
        load_fastq: str,
        password: str,
        data_by_organism_root: str = "1.Data",
        org: str = "human") -> Optional[bool]:
    """
    Load FASTQ data. Returns True if rsync was performed and successful,
    False if files already exist locally or in organism-specific folder,
    None if error occurred.

    :param flowcell               : Flowcell name.
    :param user                   : Username for SSH.
    :param save_fastq             : Path to save FASTQ data.
    :param load_fastq             : Path to load FASTQ data from ceph or remote server.
    :param password               : Password for SSH connection.
    :param data_by_organism_root  : Root path for organism-specific data (default: '1.Data').
    :param org                    : Organism name (default: 'human').
    :return                       : True if transferred/skipped, False if skipped, None if failed.
    """

    # Путь к сохранённым FASTQ-файлам
    local_fastq_path = f"{save_fastq}/{flowcell}"

    # Проверяем, есть ли уже данные в папке 1.Data/org/flowcell
    organism_fastq_path = os.path.join(data_by_organism_root, org, flowcell)

    if os.path.exists(organism_fastq_path):
        print(f"✅[Load FASTQ] Папка {organism_fastq_path} существует")
        fastq_files = glob(f"{organism_fastq_path}/**/*_R[12]*.fastq.gz", recursive=True)
        if fastq_files:
            print(f"✅[Load FASTQ] Найдено {len(fastq_files)} FASTQ-файлов в {organism_fastq_path}")
            print(f"✅[Load FASTQ] FASTQ files already exist at {organism_fastq_path}. Skipping rsync.")
            return 'Exist 1.Data'  # Файлы уже в нужном месте — ничего не делаем
        else:
            print(f"⚠️[Load FASTQ] В папке {organism_fastq_path} нет FASTQ-файлов")

    # Проверяем, есть ли локальная копия (в save_fastq)
    if os.path.exists(local_fastq_path):
        fastq_files = glob(f"{local_fastq_path}/**/*_R[12]*.fastq.gz", recursive=True)
        if fastq_files:
            print(f"✅[Load FASTQ] FASTQ files already exist at {local_fastq_path}. Skipping rsync.")
            return 'Exist 0.FASTQ'  # Уже есть локальные данные — перенос не нужен

    # Проверяем, есть ли данные на ceph или нужно брать с удалённого сервера
    fastq_flowcell_path = f"{load_fastq}/{flowcell}_fastq4"
    access_fastq = []

    if not os.path.exists(fastq_flowcell_path):
        fastq_flowcell_path = f"{load_fastq}/{flowcell}_fastq4"
        access_fastq = ["sshpass", "-p", password]

    # Создаём директорию под сохранение
    os.makedirs(local_fastq_path, exist_ok=True)

    # Команда rsync
    load_fastq_cmd = access_fastq + [
        "rsync", "-r", "--ignore-existing",
        f'{fastq_flowcell_path}/*',
        local_fastq_path
    ]

    try:
        subprocess.run(
            load_fastq_cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"✅[Load FASTQ] Rsync {local_fastq_path} complete!")
        return 'Exist 0.FASTQ'  # Перенос выполнен

    except subprocess.CalledProcessError as e:
        print(f"❌[Load FASTQ] Rsync error\nError code: {e.returncode}")
        return None  # Ошибка