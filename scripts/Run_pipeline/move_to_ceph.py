import os
import shutil
import argparse

def move_to_ceph_and_remove(work_dir: str, 
                 ceph_path: str, 
                 organism: str, 
                 flowcell: str):
    
    fastq_to_remove     = os.path.join(work_dir, "1.Data", organism, flowcell)
    source_root         = os.path.join(work_dir, "2.Results")
    source_dir          = os.path.join(source_root, organism, flowcell)
    work_dir_to_remove  = os.path.join(source_dir, 'work')
    target_dir          = os.path.join(ceph_path, organism, flowcell)

    if not os.path.exists(source_dir):
        print(f"❌[Move and remove] Error: Source directory does not exist: {source_dir}")
        return 1

    if os.path.exists(work_dir_to_remove):
        print(f"🕒[Move and remove] Removing directory: {work_dir_to_remove}")
        try:
            shutil.rmtree(work_dir_to_remove)
            print("✅[Move and remove] Directory 'work' successfully removed.")
        except Exception as e:
            print(f"❌[Move and remove] Failed to remove 'work' directory: {e}")
            return 1
    else:
        print("⚠️[Move and remove] Directory 'work' not found. Skipping removal.")

    organism_dir = os.path.join(ceph_path, organism)
    os.makedirs(organism_dir, exist_ok=True)
    print(f"✅[Move and remove] Ensured target organism directory exists: {organism_dir}")

    if os.path.exists(target_dir):
        print(f"✅[Move and remove] Target flowcell directory already exists: {target_dir}")
        print("✅[Move and remove] Merging contents...")

        all_moved = True
        for item in os.listdir(source_dir):
            src_item = os.path.join(source_dir, item)
            dst_item = os.path.join(target_dir, item)

            if os.path.exists(dst_item):
                print(f"✅[Move and remove] Item {dst_item} already exists. Skipping.")
                continue

            try:
                if os.path.isdir(src_item):
                    shutil.move(src_item, dst_item)
                else:
                    shutil.move(src_item, dst_item)
                print(f"🕒[Move and remove] Moved: {src_item} -> {dst_item}")
            except Exception as e:
                print(f"❌[Move and remove] Error moving {src_item}: {e}")
                all_moved = False
                return 1

        if all_moved and os.path.exists(source_dir):
            try:
                os.rmdir(source_dir)
                print(f"✅[Move and remove] Source directory {source_dir} has been removed.")
            except OSError:
                print(f"⚠️[Move and remove] Source directory {source_dir} was not removed because it's not empty.")
                return 1
            try:
                os.rmdir(fastq_to_remove)
                print(f"✅[Move and remove] Source directory {fastq_to_remove} has been removed.")
            except OSError:
                print(f"⚠️[Move and remove] Source directory {fastq_to_remove} was not removed because it's not empty.")
                return 1

    else:
        print(f"🕒[Move and remove] Moving entire directory from {source_dir} to {target_dir}")
        try:
            shutil.move(source_dir, target_dir)
            print(f"✅[Move and remove] Source directory {source_dir} has been moved.")
        except Exception as e:
            print(f"❌[Move and remove] Error moving directory: {e}")
            return 1

    print("✅[Move and remove] Data successfully processed and moved.")
    return 0