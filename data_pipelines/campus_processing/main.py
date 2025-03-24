import csv
import time
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from worker import process_one_row_simple

if __name__ == "__main__":
    csv_file = "/home/ziv/bbr_diff_processing/datasets/id_to_label.csv"
    file_path = "/mnt/md0/kell/tmp-ndt/mlab-context/data/15ASes_ipv4_nonnullraw.jsonl"
    output_file = "campus_standard_form.csv"
    num_workers = 40  # Adjust as needed for your system
    chunksize = 1000  # Adjust the chunk size based on your memory and performance

    start_time = time.time()
    results = []

    # Count total rows (excluding header) for the progress bar
    with open(csv_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1

    # Use a global progress bar for all tasks
    pbar = tqdm(total=total_rows, desc="Processing tasks")

    # Process CSV in chunks to avoid scheduling too many tasks at once
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        for chunk in pd.read_csv(csv_file, chunksize=chunksize):
            tasks = []
            for _, row in chunk.iterrows():
                tasks.append((row["download_uuid"], row["upload_uuid"], row["label"]))
            
            # Submit tasks for this chunk
            future_to_task = {executor.submit(process_one_row_simple, task, file_path): task for task in tasks}
            
            # Process futures as they complete and update the progress bar
            for future in as_completed(future_to_task):
                result = future.result()
                if result is not None:
                    results.append(result)
                pbar.update(1)
    pbar.close()

    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")

    # Write CSV with specified field order
    if results:
        fieldnames = [
            "id_upload", "id_download", "connection_type",
            "throughput_upload", "throughput_download", "IP",
            "series_upload", "series_download"
        ]
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)
        print(f"Wrote {len(results)} rows to '{output_file}'")
    else:
        print("No rows processed successfully.")
