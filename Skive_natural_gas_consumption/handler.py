# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 11:07:56 2023

@author: EspenNordsveen
"""


def handle(client):
    import datetime

    import pytz

    data = {
        "start_time": "2d-ago",
        "end_time": "now",
        "NGline1": ["2s=L1_BURNER_NG_FLOW:Q_TT"],
        "NGline2": ["2s=L2_BURNER_NG_FLOW:Q_TT"],
        "NGline3": ["2s=L3_BURNER_NG_FLOW:Q_TT"],
        "NGline4": ["2s=L4_BURNER_NG_FLOW:Q_TT"],
        "NGflare": ["2s=P10_QJD_BF601_DFLC01:Q_TT"],
        "el": ["2s=P10_AAW01_BJ01:M_P"],
    }

    today = datetime.date.today()
    t = datetime.time(hour=23, minute=59, second=59)

    time_now = datetime.datetime.combine(today, t) - datetime.timedelta(hours=24)

    # Find last full hour
    now = datetime.datetime.now()  # Get the current time
    last_hour = now.replace(minute=0, second=0, microsecond=0)

    dk_tz = pytz.timezone("Europe/Copenhagen")
    time_local = dk_tz.localize(time_now)

    # %%
    # Hourly NG useage
    dps_hour_first_1 = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGline1"], before=last_hour - datetime.timedelta(hours=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_hour_last_1 = (
        client.time_series.data.retrieve_latest(external_id=data["NGline1"], before=last_hour).to_pandas().iloc[0, 0]
    )
    dps_hour_first_2 = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGline2"], before=last_hour - datetime.timedelta(hours=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_hour_last_2 = (
        client.time_series.data.retrieve_latest(external_id=data["NGline2"], before=last_hour).to_pandas().iloc[0, 0]
    )
    dps_hour_first_3 = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGline3"], before=last_hour - datetime.timedelta(hours=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_hour_last_3 = (
        client.time_series.data.retrieve_latest(external_id=data["NGline3"], before=last_hour).to_pandas().iloc[0, 0]
    )
    dps_hour_first_4 = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGline4"], before=last_hour - datetime.timedelta(hours=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_hour_last_4 = (
        client.time_series.data.retrieve_latest(external_id=data["NGline4"], before=last_hour).to_pandas().iloc[0, 0]
    )
    dps_hour_first_flare = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGflare"], before=last_hour - datetime.timedelta(hours=1)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_hour_last_flare = (
        client.time_series.data.retrieve_latest(external_id=data["NGflare"], before=last_hour).to_pandas().iloc[0, 0]
    )

    # Daily NG useage
    dps_first_1 = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGline1"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_last_1 = (
        client.time_series.data.retrieve_latest(external_id=data["NGline1"], before=time_local).to_pandas().iloc[0, 0]
    )
    dps_first_2 = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGline2"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_last_2 = (
        client.time_series.data.retrieve_latest(external_id=data["NGline2"], before=time_local).to_pandas().iloc[0, 0]
    )
    dps_first_3 = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGline3"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_last_3 = (
        client.time_series.data.retrieve_latest(external_id=data["NGline3"], before=time_local).to_pandas().iloc[0, 0]
    )
    dps_first_4 = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGline4"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_last_4 = (
        client.time_series.data.retrieve_latest(external_id=data["NGline4"], before=time_local).to_pandas().iloc[0, 0]
    )
    dps_first_flare = (
        client.time_series.data.retrieve_latest(
            external_id=data["NGflare"], before=time_local - datetime.timedelta(hours=24)
        )
        .to_pandas()
        .iloc[0, 0]
    )
    dps_last_flare = (
        client.time_series.data.retrieve_latest(external_id=data["NGflare"], before=time_local).to_pandas().iloc[0, 0]
    )

    NG_last24_all = [
        dps_last_1 - dps_first_1,
        dps_last_2 - dps_first_2,
        dps_last_3 - dps_first_3,
        dps_last_4 - dps_first_4,
        dps_last_flare - dps_first_flare,
    ]
    NG_lasthour_all = [
        dps_hour_last_1 - dps_hour_first_1,
        dps_hour_last_2 - dps_hour_first_2,
        dps_hour_last_3 - dps_hour_first_3,
        dps_hour_last_4 - dps_hour_first_4,
        dps_hour_last_flare - dps_hour_first_flare,
    ]

    client.time_series.data.insert([(time_local, NG_last24_all[0])], external_id="daily_NGuseage_line1")
    client.time_series.data.insert([(time_local, NG_last24_all[1])], external_id="daily_NGuseage_line2")
    client.time_series.data.insert([(time_local, NG_last24_all[2])], external_id="daily_NGuseage_line3")
    client.time_series.data.insert([(time_local, NG_last24_all[3])], external_id="daily_NGuseage_line4")
    client.time_series.data.insert([(time_local, NG_last24_all[4])], external_id="daily_NGuseage_flare")
    client.time_series.data.insert([(last_hour, NG_lasthour_all[0])], external_id="hourly_NGuseage_line1")
    client.time_series.data.insert([(last_hour, NG_lasthour_all[1])], external_id="hourly_NGuseage_line2")
    client.time_series.data.insert([(last_hour, NG_lasthour_all[2])], external_id="hourly_NGuseage_line3")
    client.time_series.data.insert([(last_hour, NG_lasthour_all[3])], external_id="hourly_NGuseage_line4")
    client.time_series.data.insert([(last_hour, NG_lasthour_all[4])], external_id="hourly_NGuseage_flare")
