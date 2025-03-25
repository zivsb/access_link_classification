#!/usr/bin/env python3
import csv
import time
import pandas as pd
import json
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

from helpers import parse_data_string

def process_csv_row(row):
    """
    Given a row from truth_post_query.csv, parse it into a Python dict
    matching the structure of the previous dataset.
    """
    json_object = {}
    # Basic fields
    json_object['id_upload'] = row['id_upload']
    json_object['id_download'] = row['id_download']
    json_object['connection_type'] = row["network_type_upload"]

    # Parse high-level throughput info
    upload_parsed = parse_data_string(row['a_upload'])
    download_parsed = parse_data_string(row['a_download'])
    json_object['throughput_upload'] = upload_parsed['MeanThroughputMbps']
    json_object['throughput_download'] = download_parsed['MeanThroughputMbps']

    # Parse raw information
    raw_upload_parsed = parse_data_string(row['raw_upload'])
    raw_download_parsed = parse_data_string(row['raw_download'])
    json_object['IP'] = raw_upload_parsed['ClientIP']

    # Extract BBRInfo + TCPInfo arrays from raw measurements
    BBRInfo_upload = [d['BBRInfo'] for d in raw_upload_parsed['Upload']['ServerMeasurements']]
    TCPInfo_upload = [d['TCPInfo'] for d in raw_upload_parsed['Upload']['ServerMeasurements']]
    BBRInfo_download = [d['BBRInfo'] for d in raw_download_parsed['Download']['ServerMeasurements']]
    TCPInfo_download = [d['TCPInfo'] for d in raw_download_parsed['Download']['ServerMeasurements']]

    # Merge for upload
    merged_upload = []
    for bbr, tcp in zip(BBRInfo_upload, TCPInfo_upload):
        renamed_bbr = {f"bbr.{k}": v for k, v in bbr.items()}
        renamed_tcp = {f"TCP.{k}": v for k, v in tcp.items()}
        merged_upload.append({**renamed_bbr, **renamed_tcp})
    # Merge for download
    merged_download = []
    for bbr, tcp in zip(BBRInfo_download, TCPInfo_download):
        renamed_bbr = {f"bbr.{k}": v for k, v in bbr.items()}
        renamed_tcp = {f"TCP.{k}": v for k, v in tcp.items()}
        merged_download.append({**renamed_bbr, **renamed_tcp})

    # Convert merged arrays to JSON strings
    json_object["series_upload"] = json.dumps(merged_upload)
    json_object["series_download"] = json.dumps(merged_download)

    return json_object

def main():
    input_csv = "/home/ziv/bbr_diff_processing/exactlylab_data/unparsed_combined.csv"
    output_csv = "el_standard_form.csv"
    chunksize = 1000   # adjust based on available memory and expected CSV size
    num_workers = 10   # adjust based on your system

    start_time = time.time()
    results = []

    # Get total rows (excluding header) for the progress bar
    with open(input_csv, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1

    pbar = tqdm(total=total_rows, desc="Processing tasks")

    # Process the CSV in batches using ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize):
            tasks = []
            for _, row in chunk.iterrows():
                tasks.append(row)
            future_to_task = {executor.submit(process_csv_row, task): task for task in tasks}
            for future in as_completed(future_to_task):
                res = future.result()
                if res is not None:
                    results.append(res)
                pbar.update(1)
    pbar.close()

    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds.")

    # Write the parsed rows to the output CSV
    if results:
        fieldnames = list(results[0].keys())
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow(row)
        print(f"Wrote {len(results)} rows to '{output_csv}'")
    else:
        print("No rows produced. Check your input and parsing logic.")

if __name__ == "__main__":
    main()
