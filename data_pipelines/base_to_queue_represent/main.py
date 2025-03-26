import csv
import time
import json
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import argparse
import numpy as np
from scipy.stats import skew, kurtosis
import warnings


warnings.filterwarnings("ignore", category=RuntimeWarning)

def extract_features_from_series(series_list):
    """
    Given a list of dictionaries (each representing one timestamp),
    compute features for each metric key.
    Returns a dictionary of features with names formatted as key_feature.
    """
    if not series_list:
        return {}
    
    # Collect all keys from the dictionaries in the series
    keys = set()
    for item in series_list:
        keys.update(item.keys())
    keys = list(keys)

    features = {}
    # For each key, extract values and compute features
    for k in keys:
        values = []
        for item in series_list:
            if k in item:
                try:
                    values.append(float(item[k]))
                except ValueError:
                    continue
        if not values:
            continue

        arr = np.array(values)
        n = len(arr)
        features[f"{k}_count"] = n
        features[f"{k}_mean"] = np.mean(arr)
        features[f"{k}_median"] = np.median(arr)
        features[f"{k}_std"] = np.std(arr)
        features[f"{k}_min"] = np.min(arr)
        features[f"{k}_max"] = np.max(arr)
        features[f"{k}_range"] = np.max(arr) - np.min(arr)
        features[f"{k}_first"] = arr[0]
        features[f"{k}_last"] = arr[-1]
        features[f"{k}_trend"] = (arr[-1] - arr[0]) / n if n > 1 else 0
        features[f"{k}_skew"] = skew(arr)
        features[f"{k}_kurtosis"] = kurtosis(arr)
        # Linear regression slope
        x = np.arange(n)
        if n > 1:
            slope, _ = np.polyfit(x, arr, 1)
            features[f"{k}_slope"] = slope
        else:
            features[f"{k}_slope"] = 0
    return features

def process_row_for_series(row, series_key):
    """
    Parse the JSON string in the given series_key column and extract features.
    Returns a dictionary with feature names prefixed by series_key.
    """
    try:
        series_data = json.loads(row[series_key])
        feats = extract_features_from_series(series_data)
        prefixed_feats = {f"{series_key}_{k}": v for k, v in feats.items()}
        return prefixed_feats
    except Exception as e:
        print(f"Error processing row id {row.get('id_upload', 'unknown')} for {series_key}: {e}")
        return {}

def process_single_row(row):
    """
    Process one CSV row:
      - Carry over selected non-series columns.
      - Process and extract features from both 'series_upload' and 'series_download'.
    Returns a dictionary with the combined features.
    """
    row_features = {}
    for col in ['id_upload', 'id_download', 'connection_type', 'throughput_upload', 'throughput_download', 'IP']:
        row_features[col] = row.get(col, None)
    # Process the two series columns
    row_features.update(process_row_for_series(row, "series_upload"))
    row_features.update(process_row_for_series(row, "series_download"))
    return row_features

def main(input_csv, output_csv, chunksize=1000, num_workers=4):
    # Count total rows for the progress bar
    with open(input_csv, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # subtract header

    start_time = time.time()
    header_written = False

    pbar = tqdm(total=total_rows, desc="Extracting features")
    with open(output_csv, "w", newline="", encoding="utf-8") as outfile:
        writer = None
        # Process the CSV file in chunks
        for chunk in pd.read_csv(input_csv, chunksize=chunksize):
            # Convert chunk to a list of dicts (one per row)
            rows = chunk.to_dict(orient="records")
            results = []
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                # Map the process_single_row function to each row
                for res in executor.map(process_single_row, rows):
                    results.append(res)
                    pbar.update(1)
            # Write the results from this chunk
            if results:
                if not header_written:
                    header = list(results[0].keys())
                    writer = csv.DictWriter(outfile, fieldnames=header)
                    writer.writeheader()
                    header_written = True
                for r in results:
                    writer.writerow(r)
    pbar.close()
    end_time = time.time()
    print(f"Feature extraction completed in {end_time - start_time:.2f} seconds.")
    print(f"Extracted features saved to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract features from time series CSV file.")
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("output_csv", help="Path to save the output features CSV")
    parser.add_argument("--chunksize", type=int, default=1000, help="Number of rows to process at once")
    parser.add_argument("--workers", type=int, default=20, help="Number of parallel workers")
    args = parser.parse_args()
    main(args.input_csv, args.output_csv, chunksize=args.chunksize, num_workers=args.workers)
