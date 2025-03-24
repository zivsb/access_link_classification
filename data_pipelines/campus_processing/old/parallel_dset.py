import csv
import time
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from helpers import find_object_by_id_fast, compute_stats
from worker import process_one_row, process_one_row_simple, process_one_row_bb  # we still import the others if needed

if __name__ == "__main__":  # Needed for Windows or PyInstaller + multiprocessing
    df = pd.read_csv("/home/ziv/bbr_diff_processing/datasets/id_to_label.csv")
    file_path = "/mnt/md0/kell/tmp-ndt/mlab-context/data/15ASes_ipv4_nonnullraw.jsonl"

    output_file = "output_with_stats.csv"

    start_time = time.time()

    # Build a list of input tuples for each row
    tasks = []
    for _, row in df.iterrows():
        tasks.append((row["download_uuid"], row["upload_uuid"], row["label"]))

    results = []
    num_workers = 80  # or adjust as appropriate for your machine

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Use process_one_row (instead of process_one_row_bb) to extract the full stats
        future_to_row = {
            executor.submit(process_one_row, t, file_path): t for t in tasks
        }

        # Use tqdm over as_completed to show progress
        for future in tqdm(as_completed(future_to_row), total=len(tasks), desc="Parallel processing"):
            # As each task completes, get its result (this can raise exceptions if something went wrong)
            combined_row = future.result()
            if combined_row is not None:
                results.append(combined_row)
            # else skip

    end_time = time.time()
    print(f"All parallel tasks done in {end_time - start_time:.2f} seconds.")

    # Write CSV
    if results:
        # Use fieldnames from the first result
        fieldnames = list(results[0].keys())
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for rowdict in results:
                writer.writerow(rowdict)

        print(f"Wrote {len(results)} rows to '{output_file}'")
    else:
        print("No successful rows to write.")
