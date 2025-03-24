import json
import pandas as pd
from statistics import mode

def find_object_by_id_fast(file_path, target_id):
    pattern = f'{{"id":"{target_id}"'
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith(pattern):
                # Potential match found; parse and verify
                data = json.loads(line)
                if data.get('id') == target_id:
                    return data
    return None

def compute_stats(merged_measurements):
    """
    Given a list of dicts (merged BBR + TCP measurements),
    convert to a DataFrame and compute key stats.
    """
    if not merged_measurements:
        # If empty, return something safe
        return pd.Series({
            "min_rtt": None,
            "max_bw": None,
            "bdp": None,
        })

    df = pd.DataFrame(merged_measurements)

    # -----------------------------------------------------------------
    # 1) Convert relevant columns from string to numeric
    # -----------------------------------------------------------------
    df = df.apply(pd.to_numeric, errors='coerce')

    # -----------------------------------------------------------------
    # 2) Calculate main statistics
    # -----------------------------------------------------------------

    # Minimum bbr.MinRTT
    min_rtt = df["bbr.MinRTT"].min()

    # Filter rows that match the minimum MinRTT
    filtered = df[df["bbr.MinRTT"] == min_rtt]
    max_bw = filtered["bbr.BW"].max()
    bdp = min_rtt * max_bw if pd.notna(min_rtt) and pd.notna(max_bw) else None

    # -----------------------------------------------------------------
    # Calculate stats for bbr.BW
    # -----------------------------------------------------------------
    minRTT_bw_series = filtered["bbr.BW"]
    bw_median = minRTT_bw_series.median()
    bw_mean = minRTT_bw_series.mean()
    try:
        bw_mode = mode(minRTT_bw_series)
    except:
        bw_mode = None
    bw_std = minRTT_bw_series.std()
    bw_max = minRTT_bw_series.max()
    bw_min = minRTT_bw_series.min()

    # -----------------------------------------------------------------
    # Calculate stats for TCP.BytesRetrans
    # -----------------------------------------------------------------
    minRTT_bytes_retrans_series = filtered["TCP.BytesRetrans"] / bdp
    bytes_retrans_median = minRTT_bytes_retrans_series.median()
    bytes_retrans_mean = minRTT_bytes_retrans_series.mean()
    try:
        bytes_retrans_mode = mode(minRTT_bytes_retrans_series)
    except:
        bytes_retrans_mode = None
    bytes_retrans_std = minRTT_bytes_retrans_series.std()
    bytes_retrans_max = minRTT_bytes_retrans_series.max()
    bytes_retrans_min = minRTT_bytes_retrans_series.min()

    # -----------------------------------------------------------------
    # Calculate stats for TCP.Unacked
    # -----------------------------------------------------------------
    minRTT_unacked_series = filtered["TCP.Unacked"]
    unacked_median = minRTT_unacked_series.median()
    unacked_mean = minRTT_unacked_series.mean()
    try:
        unacked_mode = mode(minRTT_unacked_series)
    except:
        unacked_mode = None
    unacked_std = minRTT_unacked_series.std()
    unacked_max = minRTT_unacked_series.max()
    unacked_min = minRTT_unacked_series.min()

    # -----------------------------------------------------------------
    # Calculate stats for TCP.Lost
    # -----------------------------------------------------------------
    minRTT_lost_series = filtered["TCP.Lost"]
    lost_median = minRTT_lost_series.median()
    lost_mean = minRTT_lost_series.mean()
    try:
        lost_mode = mode(minRTT_lost_series)
    except:
        lost_mode = None
    lost_std = minRTT_lost_series.std()
    lost_max = minRTT_lost_series.max()
    lost_min = minRTT_lost_series.min()

    # -----------------------------------------------------------------
    # Calculate stats for TCP.RTT
    # -----------------------------------------------------------------
    minRTT_rtt_series = filtered["TCP.RTT"] / min_rtt
    rtt_median = minRTT_rtt_series.median()
    rtt_mean = minRTT_rtt_series.mean()
    try:
        rtt_mode = mode(minRTT_rtt_series)
    except:
        rtt_mode = None
    rtt_std = minRTT_rtt_series.std()
    rtt_max = minRTT_rtt_series.max()
    rtt_min = minRTT_rtt_series.min()

    # -----------------------------------------------------------------
    # Calculate stats for TCP.RTTVar
    # -----------------------------------------------------------------
    minRTT_rttvar_series = filtered["TCP.RTTVar"] / min_rtt
    rttvar_median = minRTT_rttvar_series.median()
    rttvar_mean = minRTT_rttvar_series.mean()
    try:
        rttvar_mode = mode(minRTT_rttvar_series)
    except:
        rttvar_mode = None
    rttvar_std = minRTT_rttvar_series.std()
    rttvar_max = minRTT_rttvar_series.max()
    rttvar_min = minRTT_rttvar_series.min()

    # -----------------------------------------------------------------
    # Elapsed time for the first entry with MinRTT
    # -----------------------------------------------------------------
    first_entry = filtered.sort_values("bbr.ElapsedTime").iloc[0]
    first_elapsed_minRTT = first_entry["bbr.ElapsedTime"]

    # -----------------------------------------------------------------
    # Ratios and differences for bbr.BW
    # -----------------------------------------------------------------
    first_bw = df["bbr.BW"].iloc[0]
    last_bw = df["bbr.BW"].iloc[-1]
    bw_first_last_ratio = first_bw / last_bw if last_bw else 0

    bw_max_all = df["bbr.BW"].max()
    bw_min_all = df["bbr.BW"].min()
    bw_max_min_ratio = bw_max_all / bw_min_all if bw_min_all else 0
    bw_max_min_diff = bw_max_all - bw_min_all

    # -----------------------------------------------------------------
    # Calculate the stats for TCP.SndCwnd
    # -----------------------------------------------------------------
    minRTT_sndcwnd_series = filtered["TCP.SndCwnd"]
    sndcwnd_median = minRTT_sndcwnd_series.median()
    sndcwnd_mean = minRTT_sndcwnd_series.mean()
    try:
        sndcwnd_mode = mode(minRTT_sndcwnd_series)
    except:
        sndcwnd_mode = None
    sndcwnd_std = minRTT_sndcwnd_series.std()
    sndcwnd_max = minRTT_sndcwnd_series.max()
    sndcwnd_min = minRTT_sndcwnd_series.min()

    # -----------------------------------------------------------------
    # Return as a pandas Series
    # -----------------------------------------------------------------
    return pd.Series({
        "min_rtt":                min_rtt,
        "max_bw":                 max_bw,
        "bdp":                    bdp,

        "bw_median":              bw_median,
        "bw_mean":                bw_mean,
        "bw_mode":                bw_mode,
        "bw_std":                 bw_std,
        "bw_max":                 bw_max,
        "bw_min":                 bw_min,

        "bytes_retrans_median":   bytes_retrans_median,
        "bytes_retrans_mean":     bytes_retrans_mean,
        "bytes_retrans_mode":     bytes_retrans_mode,
        "bytes_retrans_std":      bytes_retrans_std,
        "bytes_retrans_max":      bytes_retrans_max,
        "bytes_retrans_min":      bytes_retrans_min,

        "unacked_median":         unacked_median,
        "unacked_mean":           unacked_mean,
        "unacked_mode":           unacked_mode,
        "unacked_std":            unacked_std,
        "unacked_max":            unacked_max,
        "unacked_min":            unacked_min,

        "lost_median":            lost_median,
        "lost_mean":              lost_mean,
        "lost_mode":              lost_mode,
        "lost_std":               lost_std,
        "lost_max":               lost_max,
        "lost_min":               lost_min,

        "rtt_median":             rtt_median,
        "rtt_mean":               rtt_mean,
        "rtt_mode":               rtt_mode,
        "rtt_std":                rtt_std,
        "rtt_max":                rtt_max,
        "rtt_min":                rtt_min,

        "rttvar_median":          rttvar_median,
        "rttvar_mean":            rttvar_mean,
        "rttvar_mode":            rttvar_mode,
        "rttvar_std":             rttvar_std,
        "rttvar_max":             rttvar_max,
        "rttvar_min":             rttvar_min,

        "sndcwnd_median":         sndcwnd_median,
        "sndcwnd_mean":           sndcwnd_mean,
        "sndcwnd_mode":           sndcwnd_mode,
        "sndcwnd_std":            sndcwnd_std,
        "sndcwnd_max":            sndcwnd_max,
        "sndcwnd_min":            sndcwnd_min,

        "minrtt_first_elapsed":   first_elapsed_minRTT,
        "bw_first_last_ratio":    bw_first_last_ratio,
        "bw_max_min_ratio":       bw_max_min_ratio,
        "bw_max_min_diff":        bw_max_min_diff,
    })

