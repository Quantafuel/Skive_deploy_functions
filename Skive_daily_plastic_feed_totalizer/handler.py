# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 12:16:21 2022

@author: EspenNordsveen
"""


def handle(client):
    import datetime

    import pytz

    data = {
        "plastamount1": ["2s=P01_EAC_BW002_DFLC01:Q_TT"],
        "plastamount2": ["2s=P02_EAC_BW002_DFLC01:Q_TT"],
        "plastamount3": ["2s=P03_EAC_BW002_DFLC01:Q_TT"],
        "plastamount4": ["2s=P04_EAC_BW002_DFLC01:Q_TT"],
    }
    today = datetime.date.today()
    t = datetime.time(hour=23, minute=59, second=59)

    time_now = datetime.datetime.combine(today, t) - datetime.timedelta(hours=24)

    dk_tz = pytz.timezone("Europe/Copenhagen")
    time_local = dk_tz.localize(time_now)

    df_dps_24_1 = (
        client.time_series.data.retrieve_latest(
            external_id=data["plastamount1"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_1 = (
        client.time_series.data.retrieve_latest(external_id=data["plastamount1"], before=time_local)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_2 = (
        client.time_series.data.retrieve_latest(
            external_id=data["plastamount2"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_2 = (
        client.time_series.data.retrieve_latest(external_id=data["plastamount2"], before=time_local)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_3 = (
        client.time_series.data.retrieve_latest(
            external_id=data["plastamount3"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_3 = (
        client.time_series.data.retrieve_latest(external_id=data["plastamount3"], before=time_local)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_24_4 = (
        client.time_series.data.retrieve_latest(
            external_id=data["plastamount4"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_4 = (
        client.time_series.data.retrieve_latest(external_id=data["plastamount4"], before=time_local)
        .to_pandas()
        .iloc[0, 0]
    )

    plasticamount_last24_all = [
        df_dps_now_1 - df_dps_24_1,
        df_dps_now_2 - df_dps_24_2,
        df_dps_now_3 - df_dps_24_3,
        df_dps_now_4 - df_dps_24_4,
    ]

    client.time_series.data.insert(
        [(time_local, plasticamount_last24_all[0])], external_id="Daily_plastic_amount_line1"
    )
    client.time_series.data.insert(
        [(time_local, plasticamount_last24_all[1])], external_id="Daily_plastic_amount_line2"
    )
    client.time_series.data.insert(
        [(time_local, plasticamount_last24_all[2])], external_id="Daily_plastic_amount_line3"
    )
    client.time_series.data.insert(
        [(time_local, plasticamount_last24_all[3])], external_id="Daily_plastic_amount_line4"
    )

    return plasticamount_last24_all
