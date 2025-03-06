# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 09:26:57 2022

@author: EspenNordsveen
"""


def handle(client):
    import datetime

    import pytz

    data = {
        "start_time": "1d-ago",
        "end_time": "now",
        "productionhours1": ["2s=L1_PRODUCTION_HOURS:Q_TT"],
        "productionhours2": ["2s=L2_PRODUCTION_HOURS:Q_TT"],
        "productionhours3": ["2s=L3_PRODUCTION_HOURS:Q_TT"],
        "productionhours4": ["2s=L4_PRODUCTION_HOURS:Q_TT"],
    }
    today = datetime.date.today()
    t = datetime.time(hour=23, minute=59, second=59)

    time_now = datetime.datetime.combine(today, t) - datetime.timedelta(hours=24)

    dk_tz = pytz.timezone("Europe/Copenhagen")
    time_local = dk_tz.localize(time_now)

    df_dps_24_1 = (
        client.time_series.data.retrieve_latest(
            external_id=data["productionhours1"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_1 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours1"], before=time_local)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_2 = (
        client.time_series.data.retrieve_latest(
            external_id=data["productionhours2"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_2 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours2"], before=time_local)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_3 = (
        client.time_series.data.retrieve_latest(
            external_id=data["productionhours3"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_3 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours3"], before=time_local)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_4 = (
        client.time_series.data.retrieve_latest(
            external_id=data["productionhours4"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_4 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours4"], before=time_local)
        .to_pandas()
        .iloc[0, 0]
    )

    operating_last24_all = [
        df_dps_now_1 - df_dps_24_1,
        df_dps_now_2 - df_dps_24_2,
        df_dps_now_3 - df_dps_24_3,
        df_dps_now_4 - df_dps_24_4,
    ]

    client.time_series.data.insert([(time_local, operating_last24_all[0])], external_id="Hourly_production_line1")
    client.time_series.data.insert([(time_local, operating_last24_all[1])], external_id="Hourly_production_line2")
    client.time_series.data.insert([(time_local, operating_last24_all[2])], external_id="Hourly_production_line3")
    client.time_series.data.insert([(time_local, operating_last24_all[3])], external_id="Hourly_production_line4")

    return operating_last24_all
