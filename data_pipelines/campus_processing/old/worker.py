# worker.py or the same script
import json
from helpers import find_object_by_id_fast, compute_stats

def process_one_row(row, file_path):
    """
    row: a tuple (download_uuid, upload_uuid, label)
    file_path: path to the large JSONL file
    Returns: a dictionary for CSV writing (or None if something's missing)
    """
    (download_uuid, upload_uuid, label) = row

    # 1) Find the objects
    download_object = find_object_by_id_fast(file_path, download_uuid)
    if download_object is None:
        return None  # Or return some indicator that we failed

    upload_object = find_object_by_id_fast(file_path, upload_uuid)
    if upload_object is None:
        return None

    # 2) Extract server measurements
    download_server_measurements = download_object["raw"]["Download"]["ServerMeasurements"]
    upload_server_measurements   = upload_object["raw"]["Upload"]["ServerMeasurements"]

    # 3) Merge BBR + TCP info
    BBRInfo_download = [d['BBRInfo'] for d in download_server_measurements]
    BBRInfo_upload   = [u['BBRInfo'] for u in upload_server_measurements]
    TCPInfo_download = [d['TCPInfo'] for d in download_server_measurements]
    TCPInfo_upload   = [u['TCPInfo'] for u in upload_server_measurements]

    merged_upload = []
    for bbr, tcp in zip(BBRInfo_upload, TCPInfo_upload):
        renamed_bbr = {f"bbr.{k}": v for k, v in bbr.items()}
        renamed_tcp = {f"TCP.{k}": v for k, v in tcp.items()}
        merged_upload.append({**renamed_bbr, **renamed_tcp})

    merged_download = []
    for bbr, tcp in zip(BBRInfo_download, TCPInfo_download):
        renamed_bbr = {f"bbr.{k}": v for k, v in bbr.items()}
        renamed_tcp = {f"TCP.{k}": v for k, v in tcp.items()}
        merged_download.append({**renamed_bbr, **renamed_tcp})

    # 4) Compute stats
    download_stats = compute_stats(merged_download)
    upload_stats   = compute_stats(merged_upload)

    # 5) Build a dictionary for CSV
    combined_row = {
        "download_uuid": download_uuid,
        "upload_uuid":   upload_uuid,
        "label":         label,
    }

    # Add download stats with a prefix
    for stat_name, stat_value in download_stats.items():
        combined_row[f"download_{stat_name}"] = stat_value

    # Add upload stats with a prefix
    for stat_name, stat_value in upload_stats.items():
        combined_row[f"upload_{stat_name}"] = stat_value

    return combined_row


def process_one_row_simple(row, file_path):
    """
    row: a tuple (download_uuid, upload_uuid, label)
    file_path: path to the large JSONL file
    Returns: a dictionary for CSV writing (or None if something's missing)
    """
    (download_uuid, upload_uuid, label) = row

    # Find the objects
    download_object = find_object_by_id_fast(file_path, download_uuid)
    if download_object is None:
        return None

    upload_object = find_object_by_id_fast(file_path, upload_uuid)
    if upload_object is None:
        return None

    # Extract raw ServerMeasurements
    download_server_measurements = download_object["raw"]["Download"]["ServerMeasurements"]
    upload_server_measurements = upload_object["raw"]["Upload"]["ServerMeasurements"]

    # Extract the throughputs
    throughput_download = download_object["a"]["MeanThroughputMbps"]
    throughput_upload = upload_object["a"]["MeanThroughputMbps"]

    # Extract the IP address
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

    # 4) Instead of computing stats, store the entire merged arrays as JSON strings
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
            except:
                pass
            return obj

    cleaned_download = convert_numeric_strings(merged_download)
    cleaned_upload   = convert_numeric_strings(merged_upload)

    download_json = json.dumps(cleaned_download)
    upload_json   = json.dumps(cleaned_upload)

    # 5) Build the final dictionary for this row
    # Need 'id_upload', 'id_download', 'connection_type', 'throughput_upload', 'throughput_download', 'IP', 'series_upload', 'series_download'
    combined_row = {
        "id_upload":           upload_uuid,
        "id_download":         download_uuid,
        "connection_type":     label,
        "throughput_upload":   throughput_download,
        "throughput_download": throughput_upload,
        "IP":                  client_ip,
        "series_upload":         upload_json,
        "series_download":       download_json,
    }

    return combined_row

def process_one_row_bb(row, file_path):
    """
    row: a tuple (download_uuid, upload_uuid, label)
    file_path: path to the large JSONL file
    Returns: a dictionary for CSV writing (or None if something's missing)
    """
    (download_uuid, upload_uuid, label) = row

    # Find the objects
    download_object = find_object_by_id_fast(file_path, download_uuid)
    if download_object is None:
        return None

    upload_object = find_object_by_id_fast(file_path, upload_uuid)
    if upload_object is None:
        return None

    # Extract the throughputs
    throughput_download = download_object["a"]["MeanThroughputMbps"]
    throughput_upload = upload_object["a"]["MeanThroughputMbps"]

    # Extract the MinRTT
    MinRTT_download = download_object["a"]["MinRTT"]
    MinRTT_upload = upload_object["a"]["MinRTT"]

    # Extract the IP address
    client_ip = upload_object["raw"]["ClientIP"]

    # Combine stats into a single dictionary
    combined_row = {
        "id_upload":           upload_uuid,
        "id_download":         download_uuid,
        "IP":                  client_ip,
        "connection_type":     label,
        "throughput_upload":   throughput_download,
        "throughput_download": throughput_upload,
        "MinRTT_upload":       MinRTT_upload,
        "MinRTT_download":     MinRTT_download
    }

    return combined_row