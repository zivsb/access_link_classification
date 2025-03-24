import csv
import time
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from worker import process_one_row_simple

if __name__ == "__main__":
    # Update the path as needed
    df = pd.read_csv("/home/ziv/bbr_diff_processing/datasets/id_to_label.csv")
    file_path = "/mnt/md0/kell/tmp-ndt/mlab-context/data/15ASes_ipv4_nonnullraw.jsonl"
    output_file = "testing/campus_standard_form.csv"  # Test output file

    # Use only the first 10 rows for a quick test run
    df_test = df.head(10)

    start_time = time.time()

    tasks = []
    for _, row in df_test.iterrows():
        tasks.append((row["download_uuid"], row["upload_uuid"], row["label"]))

    results = []
    num_workers = 10  # Fewer workers for a small test
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_task = {executor.submit(process_one_row_simple, task, file_path): task for task in tasks}
        for future in tqdm(as_completed(future_to_task), total=len(tasks), desc="Testing"):
            result = future.result()
            if result is not None:
                results.append(result)

    end_time = time.time()
    print(f"Test completed in {end_time - start_time:.2f} seconds.")

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
        print(f"Wrote {len(results)} rows to '{output_file}' (Test Output)")
    else:
        print("No rows processed successfully in test.")
