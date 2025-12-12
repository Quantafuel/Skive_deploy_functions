# -*- coding: utf-8 -*-
"""
Created on Fri Dec  5 12:52:18 2025

@author: EspenNordsveen
"""


def handle(client):
    from datetime import datetime, timezone

    LINES = [1, 2, 3, 4]
    CLUSTERS = [1, 2, 3, 4, 5, 6]

    def update_line_and_clusters(line: int):
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)

        # -------------------------
        # Get current line status
        # -------------------------
        try:
            status_dp = client.time_series.data.retrieve_latest(external_id=f"LIVE_STATUS_LINE_{line}")[0]
            is_running = status_dp.value == 6
        except Exception as e:
            print(e)
            is_running = False

        cluster_values = []

        # -------------------------
        # Update each cluster
        # -------------------------
        for cluster in CLUSTERS:
            xid = f"hgf_run_time_cluster_{line}_{cluster}"

            try:
                last_dp = client.time_series.data.retrieve_latest(external_id=xid)[0]
                last_value = float(last_dp.value)
                last_time_ms = last_dp.timestamp
            except Exception as e:
                print(e)
                last_value = 0.0
                last_time_ms = now_ms

            delta_sec = (now_ms - last_time_ms) / 1000.0

            next_value = last_value + delta_sec if is_running else last_value

            client.time_series.data.insert(external_id=xid, datapoints=[(now_ms, next_value)])
            print(f"Wrote {next_value:.2f} seconds to {xid}")
            cluster_values.append(next_value)

        # -------------------------
        # Line runtime = min(cluster)
        # -------------------------
        line_runtime = min(cluster_values)

        client.time_series.data.insert(
            external_id=f"hgf_total_run_time_line_{line}", datapoints=[(now_ms, line_runtime)]
        )

        return line_runtime

    for line in LINES:
        runtime = update_line_and_clusters(line)
        print(f"Line {line}: total runtime = {runtime:.1f} s")