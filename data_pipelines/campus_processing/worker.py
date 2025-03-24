import json
from helpers import find_object_by_id_fast

def process_one_row_simple(row, file_path):
    """
    Process a single row and return a dictionary with:
      id_upload, id_download, connection_type,
      throughput_upload, throughput_download, IP,
      series_upload, and series_download.
    
    The merged series are converted to JSON strings with numeric values
    converted from strings.
    
    Parameters:
      row: tuple (download_uuid, upload_uuid, connection_type)
      file_path: path to the large JSONL file
    
    Returns:
      dict with the required fields or None if a required object is missing.
    """
    (download_uuid, upload_uuid, connection_type) = row

    # Find the JSON objects using the helper
    download_object = find_object_by_id_fast(file_path, download_uuid)
    if download_object is None:
        return None

    upload_object = find_object_by_id_fast(file_path, upload_uuid)
    if upload_object is None:
        return None

    # Extract raw ServerMeasurements
    download_server_measurements = download_object["raw"]["Download"]["ServerMeasurements"]
    upload_server_measurements = upload_object["raw"]["Upload"]["ServerMeasurements"]

    # Extract throughputs
    throughput_download = download_object["a"]["MeanThroughputMbps"]
    throughput_upload = upload_object["a"]["MeanThroughputMbps"]

    # Extract IP address
    client_ip = upload_object["raw"]["ClientIP"]

    # Merge BBR + TCP data for each measurement
    BBRInfo_download = [d['BBRInfo'] for d in download_server_measurements]
    TCPInfo_download = [d['TCPInfo'] for d in download_server_measurements]
    BBRInfo_upload   = [u['BBRInfo'] for u in upload_server_measurements]
    TCPInfo_upload   = [u['TCPInfo'] for u in upload_server_measurements]

    merged_download = []
    for bbr, tcp in zip(BBRInfo_download, TCPInfo_download):
        renamed_bbr = {f"bbr.{k}": v for k, v in bbr.items()}
        renamed_tcp = {f"TCP.{k}": v for k, v in tcp.items()}
        merged_download.append({**renamed_bbr, **renamed_tcp})

    merged_upload = []
    for bbr, tcp in zip(BBRInfo_upload, TCPInfo_upload):
        renamed_bbr = {f"bbr.{k}": v for k, v in bbr.items()}
        renamed_tcp = {f"TCP.{k}": v for k, v in tcp.items()}
        merged_upload.append({**renamed_bbr, **renamed_tcp})

    # Helper function to convert numeric strings to numbers
    def convert_numeric_strings(obj):
        if isinstance(obj, dict):
            return {k: convert_numeric_strings(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_numeric_strings(e) for e in obj]
        else:
            try:
                if isinstance(obj, str) and obj.strip() != "":
                    n = float(obj)
                    return int(n) if n.is_integer() else n
            except Exception:
                pass
            return obj

    # Convert numeric strings in the merged data
    cleaned_download = convert_numeric_strings(merged_download)
    cleaned_upload = convert_numeric_strings(merged_upload)

    # Convert the cleaned data to JSON strings.
    download_json = json.dumps(cleaned_download)
    upload_json = json.dumps(cleaned_upload)

    # Build final output dictionary
    combined_row = {
        "id_upload": upload_uuid,
        "id_download": download_uuid,
        "connection_type": connection_type,
        "throughput_upload": throughput_upload,
        "throughput_download": throughput_download,
        "IP": client_ip,
        "series_upload": upload_json,
        "series_download": upload_json,
    }
    # Note: Make sure to use upload_json for series_upload? If that was a typo, use upload_json and download_json appropriately.
    # For example, if series_upload should come from the upload measurements, then:
    combined_row["series_upload"] = upload_json
    combined_row["series_download"] = download_json

    return combined_row
