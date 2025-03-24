import csv
import time
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from helpers import find_object_by_id_fast, compute_stats
from worker import process_one_row  # or process_one_row_simple/process_one_row_bb as needed

def main():
    # Read the complete CSV and limit to the first 10 rows for testing
    df = pd.read_csv("/home/ziv/bbr_diff_processing/datasets/id_to_label.csv")
    df_test = df.head(10)
    
    # File path to the huge JSONL file
    file_path = "/mnt/md0/kell/tmp-ndt/mlab-context/data/15ASes_ipv4_nonnullraw.jsonl"
    
    # Output file for the test run
    output_file = "test_output.csv"
    
    start_time = time.time()
    
    # Build a list of tasks using only the test data points
    tasks = []
    for _, row in df_test.iterrows():
        tasks.append((row["download_uuid"], row["upload_uuid"], row["label"]))
    
    results = []
    # Use a smaller number of workers for a quick test run; 10 is enough since we only have 10 tasks
    num_workers = min(10, 80)
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_task = {
            executor.submit(process_one_row, task, file_path): task for task in tasks
        }
        for future in tqdm(as_completed(future_to_task), total=len(tasks), desc="Parallel processing"):
            try:
                combined_row = future.result()
                if combined_row is not None:
                    results.append(combined_row)
            except Exception as e:
                print(f"Error processing task {future_to_task[future]}: {e}")
    
    end_time = time.time()
    print(f"All parallel tasks done in {end_time - start_time:.2f} seconds.")
    
    # Write the results to the output CSV if there are any successful rows
    if results:
        fieldnames = list(results[0].keys())
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for rowdict in results:
                writer.writerow(rowdict)
        print(f"Wrote {len(results)} rows to '{output_file}'")
    else:
        print("No successful rows to write.")

if __name__ == "__main__":
    main()
