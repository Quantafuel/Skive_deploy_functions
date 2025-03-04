# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 09:18:58 2022

@author: EspenUlsetNordsveen
"""


def handle(client):
    import datetime

    # import pandas as pd
    import pytz

    # import pandas
    data = {
        "start_time": "1d-ago",
        "end_time": "now",
        "productionhours1": ["2s=L1_PRODUCTION_HOURS:Q_TT"],
        "productionhours2": ["2s=L2_PRODUCTION_HOURS:Q_TT"],
        "productionhours3": ["2s=L3_PRODUCTION_HOURS:Q_TT"],
        "productionhours4": ["2s=L4_PRODUCTION_HOURS:Q_TT"],
    }
    d = datetime.datetime.today()
    d = d - datetime.timedelta(days=d.weekday())
    dt = d - datetime.timedelta(days=1)
    starttime = dt.replace(hour=23, minute=59, second=59)
    dk_tz = pytz.timezone("Europe/Copenhagen")
    time_now_DKlocal = dk_tz.localize(starttime)

    df_dps_24_1_first = (
        client.time_series.data.retrieve_latest(
            external_id=data["productionhours1"], before=time_now_DKlocal - datetime.timedelta(weeks=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_1_first = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours1"], before=time_now_DKlocal)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_2_first = (
        client.time_series.data.retrieve_latest(
            external_id=data["productionhours2"], before=time_now_DKlocal - datetime.timedelta(weeks=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_2_first = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours2"], before=time_now_DKlocal)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_3_first = (
        client.time_series.data.retrieve_latest(
            external_id=data["productionhours3"], before=time_now_DKlocal - datetime.timedelta(weeks=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_3_first = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours3"], before=time_now_DKlocal)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_4_first = (
        client.time_series.data.retrieve_latest(
            external_id=data["productionhours4"], before=time_now_DKlocal - datetime.timedelta(weeks=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_4_first = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours4"], before=time_now_DKlocal)
        .to_pandas()
        .iloc[0, 0]
    )

    operating_last24_all_start = [
        df_dps_now_1_first - df_dps_24_1_first,
        df_dps_now_2_first - df_dps_24_2_first,
        df_dps_now_3_first - df_dps_24_3_first,
        df_dps_now_4_first - df_dps_24_4_first,
    ]
    dps = [
        (time_now_DKlocal, operating_last24_all_start[0]),
        (time_now_DKlocal, operating_last24_all_start[1]),
        (time_now_DKlocal, operating_last24_all_start[2]),
        (time_now_DKlocal, operating_last24_all_start[3]),
    ]

    client.time_series.data.insert([dps[0]], external_id="weekly_uptime_line1")
    client.time_series.data.insert([dps[1]], external_id="weekly_uptime_line2")
    client.time_series.data.insert([dps[2]], external_id="weekly_uptime_line3")
    client.time_series.data.insert([dps[3]], external_id="weekly_uptime_line4")

    return operating_last24_all_start
