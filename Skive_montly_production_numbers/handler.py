# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 09:18:58 2022

@author: EspenUlsetNordsveen
"""


def handle(client):
    from datetime import datetime, timedelta

    import pytz

    data = {
        "start_time": "1d-ago",
        "end_time": "now",
        "productionhours1": ["2s=L1_PRODUCTION_HOURS:Q_TT"],
        "productionhours2": ["2s=L2_PRODUCTION_HOURS:Q_TT"],
        "productionhours3": ["2s=L3_PRODUCTION_HOURS:Q_TT"],
        "productionhours4": ["2s=L4_PRODUCTION_HOURS:Q_TT"],
    }

    dk_tz = pytz.timezone("Europe/Copenhagen")
    today = datetime.today()
    t = today.replace(day=1, hour=0, minute=0, second=0)
    t = t - timedelta(days=1)
    time_now = t.replace(hour=23, minute=59, second=59)
    time_local = dk_tz.localize(time_now)

    # Get current time and localize
    now = datetime.now(dk_tz)

    # Get the last day of the previous month at 23:59:59
    last_month_end = (now.replace(day=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)

    # Get the same time one month earlier
    if last_month_end.month > 1:
        one_month_ago = last_month_end.replace(month=last_month_end.month - 1)
    else:
        one_month_ago = last_month_end.replace(year=last_month_end.year - 1, month=12)

    # Ensure timestamps are in datetime format
    df_dps_24_1 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours1"], before=one_month_ago)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_1 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours1"], before=last_month_end)
        .to_pandas()
        .iloc[0, 0]
    )

    df_dps_24_2 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours2"], before=one_month_ago)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_2 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours2"], before=last_month_end)
        .to_pandas()
        .iloc[0, 0]
    )

    df_dps_24_3 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours3"], before=one_month_ago)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_3 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours3"], before=last_month_end)
        .to_pandas()
        .iloc[0, 0]
    )

    df_dps_24_4 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours4"], before=one_month_ago)
        .to_pandas()
        .iloc[0, 0]
    )
    df_dps_now_4 = (
        client.time_series.data.retrieve_latest(external_id=data["productionhours4"], before=last_month_end)
        .to_pandas()
        .iloc[0, 0]
    )

    operating_last24_all = [
        df_dps_now_1 - df_dps_24_1,
        df_dps_now_2 - df_dps_24_2,
        df_dps_now_3 - df_dps_24_3,
        df_dps_now_4 - df_dps_24_4,
    ]

    client.time_series.data.insert([(time_local, operating_last24_all[0])], external_id="montly_production_hours_line1")
    client.time_series.data.insert([(time_local, operating_last24_all[1])], external_id="montly_production_hours_line2")
    client.time_series.data.insert([(time_local, operating_last24_all[2])], external_id="montly_production_hours_line3")
    client.time_series.data.insert([(time_local, operating_last24_all[3])], external_id="montly_production_hours_line4")

    return operating_last24_all
