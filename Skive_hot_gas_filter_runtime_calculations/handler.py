# -*- coding: utf-8 -*-
"""
Created on Fri Dec  5 12:52:18 2025

@author: EspenNordsveen
"""


def handle(client):
    from datetime import datetime

    time_now = datetime.now()
    time_now_ms = int(time_now.timestamp() * 1000)

    lines = [1, 2, 3, 4]
    clusters = [1, 2, 3, 4, 5, 6]
    for line in lines:
        cluster_run_times = []
        for cluster in clusters:
            xid = f"hgf_run_time_cluster_{line}_{cluster}"
            try:
                last_dp = client.time_series.data.retrieve_latest(external_id=xid)[0]
                last_dp_value = last_dp.value
                last_dp_time_sec = last_dp.timestamp
            except Exception as e:
                last_dp_value = None
                last_dp_time_sec = None
                print(f"{xid}: no datapoints yet:", e)
            if last_dp_value is None:
                next_val = 0
            else:
                delta_seconds = (time_now_ms - last_dp_time_sec) / 1000
                next_val = delta_seconds + last_dp_value

            cluster_run_times.append(next_val)

            client.time_series.data.insert(external_id=xid, datapoints=[(time_now_ms, next_val)])

            print(f"Wrote {next_val:.2f} seconds to {xid} at {time_now_ms}")

        client.time_series.data.insert(
            external_id=f"hgf_total_run_time_line_{line}", datapoints=[(time_now_ms, min(cluster_run_times))]
        )
        print(f"Wrote {min(cluster_run_times):.2f} seconds to hgf_total_run_time_line_{line} at {time_now_ms}")
