# -*- coding: utf-8 -*-
"""
Created on Thu Apr 13 13:11:30 2023

@author: EspenNordsveen
"""

from datetime import datetime

# from cog_client import client
import pandas as pd


# data = {"line1": "2s=P01EAC01GL001M101:M_HAST",
#         "line2": "2s=P02EAC02GL001M201:M_HAST",
#         "line3": "2s=P03EAC03GL001M301:M_HAST",
#         "line4": "2s=P04EAC04GL001M401:M_HAST"}


def handle(data, client):
    time_now = datetime.now()
    # dk_tz = pytz.timezone('Europe/Copenhagen')
    # time_local = dk_tz.localize(time_now)

    df = client.datapoints.retrieve_latest(
        external_id=[data["line1"], data["line2"], data["line3"], data["line4"]], before=time_now
    ).to_pandas()

    max_values = df.apply(lambda x: x[x == x.max()].values[0], axis=1)
    max_values = max_values[~pd.isnull(max_values)]

    lines_running = 0

    for i in max_values:
        if i > 0:
            lines_running += 1

    client.datapoints.insert([(time_now, lines_running)], external_id="number_lines_running")
    return lines_running
