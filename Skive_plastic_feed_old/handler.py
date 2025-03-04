# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 08:12:13 2022

@author: SébastienPissot
"""


# %%
# from cognite.client.data_classes import TimeSeries


# from cog_client import get_client
# client = get_client()


def handle(client):
    """
    Calculates three time series from the below formula
    Oil_produced = Heavy_oil + Light_oil
    Plastic_feed = (Heavy_oil + Light_oil + NCG)/0.9 + Water
    Water = Water_Module_1 + Water_Module_2
    :param data:
    :param client:
    :return:
    """
    import numpy as np
    import pandas as pd

    from cognite.client.data_classes import TimeSeriesWrite

    def create_timeseries(client, pf, op, wp):
        asset = client.assets.retrieve(external_id="Quantafuel_Skive")
        if pf is None:
            ts1 = TimeSeriesWrite(
                external_id="Plastic_feed",
                name="Plastic_feed",
                metadata={"Info": "Calculated from cognite functions"},
                asset_id=asset.id,
                description="Calculated from the formula (Light_oil+Heavy_oil+NCG)/0.9+Water",
            )
            client.time_series.create(ts1)
        if op is None:
            ts2 = TimeSeriesWrite(
                external_id="Oil_produced",
                name="Oil_produced",
                metadata={"Info": "Calculated from cognite functions"},
                asset_id=asset.id,
                description="Calculated from the formula (Light_oil+Heavy_oil)",
            )
            client.time_series.create(ts2)

        if wp is None:
            ts3 = TimeSeriesWrite(
                external_id="Water_separated",
                name="Water_separated",
                metadata={"Info": "Calculated from cognite functions"},
                asset_id=asset.id,
                description="Water separated. Sum of 2s=P11_GNK_BF925_MFLW01:M_MID and '2s=P12_GNK_BF975_MFLW01:M_MID",
            )
            client.time_series.create(ts3)

    data = {
        "agg1": "stepInterpolation",
        "agg2": "average",
        "start_time": "10d-ago",
        "end_time": "now",
        "gran": "1m",
        "low_threshold": 5,
        "cond_pump_threshold": 10,
        "ncg": ["2s=P10_EKG_BF001_MFLW01:M_MID"],
        "light_oil": ["2s=P11_EGG_BF924_MFLW01:M_MID", "2s=P12_EGG_BF974_MFLW01:M_MID"],
        "heavy_oil": [
            "2s=P01_EGG_BF102_MFLW01:M_MID",
            "2s=P02_EGG_BF202_MFLW01:M_MID",
            "2s=P03_EGG_BF302_MFLW01:M_MID",
            "2s=P04_EGG_BF402_MFLW01:M_MID",
        ],
        "heavy_oil_cond": [
            "2s=P01_EGG_BF103_MFLW01:M_MID",
            "2s=P02_EGG_BF203_MFLW01:M_MID",
            "2s=P03_EGG_BF303_MFLW01:M_MID",
            "2s=P04_EGG_BF403_MFLW01:M_MID",
        ],
        "cond_pump": [
            "2s=P01_EGG_GP101:M_SPD",
            "2s=P02_EGG_GP201:M_SPD",
            "2s=P03_EGG_GP301:M_SPD",
            "2s=P04_EGG_GP401:M_SPD",
        ],
        "water": ["2s=P11_GNK_BF925_MFLW01:M_MID", "2s=P12_GNK_BF975_MFLW01:M_MID"],
    }

    print(data)
    all_ts_list = data["light_oil"].copy()
    all_ts_list.extend(data["heavy_oil"].copy())
    all_ts_list.extend(data["heavy_oil_cond"].copy())
    all_ts_list.extend(data["cond_pump"].copy())
    all_ts_list.extend(data["water"].copy())

    client.assets.list(limit=1)
    data_all = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=[data["agg1"]],
        granularity=data["gran"],
    )
    data_all.columns = all_ts_list

    # Data with average aggregation
    all_ts_list_avg = data["ncg"].copy()
    data_all_avg = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list_avg,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=[data["agg2"]],
        granularity=data["gran"],
    )
    data_all_avg.columns = all_ts_list_avg

    data_all = pd.concat([data_all_avg, data_all], axis="columns")

    # Retrieve latest datapoint if first datapoint of a timeseries is NaN
    for column in data_all.columns:
        if np.isnan(data_all[column][0]):
            data_all[column][0] = (
                client.time_series.data.retrieve_latest(external_id=column, before=data["start_time"])
                .to_pandas()
                .iloc[0, 0]
            )
    data_all[data_all < 1] = 0  # Removing noise
    data_all = data_all.fillna(method="ffill")

    # Filter out wrong (high signal) HO condensate values
    for line in [0, 1, 2, 3]:
        data_all[data["heavy_oil_cond"][line]][data_all[data["cond_pump"][line]] < data["cond_pump_threshold"]] = 0

    # ss = data_all[data["screw_speeds"]].sum(axis=1)
    pf_df = (
        data_all[data["light_oil"] + data["heavy_oil"] + data["heavy_oil_cond"] + data["ncg"]].sum(axis=1).to_frame()
        / 0.9
    )
    pf_df.columns = ["oil_sum"]
    pf_df["water"] = data_all[data["water"]].sum(axis=1)
    df_all = pf_df.sum(axis=1).to_frame()
    df_all.columns = ["Plastic_feed"]
    df_all["Water_separated"] = pf_df["water"]
    df_all["Oil_produced"] = (
        data_all[data["light_oil"] + data["heavy_oil"] + data["heavy_oil_cond"]].sum(axis=1).to_frame()
    )

    df_all.loc[df_all["Oil_produced"] < data["low_threshold"], "Oil_produced"] = 0
    df_all.loc[df_all["Water_separated"] < data["low_threshold"], "Water_separated"] = 0
    df_all.loc[df_all["Plastic_feed"] < data["low_threshold"], "Plastic_feed"] = 0
    dps_len = len(df_all)
    print(dps_len)
    resp_pf = client.time_series.retrieve(external_id="Plastic_feed")
    resp_op = client.time_series.retrieve(external_id="Oil_produced")
    resp_wp = client.time_series.retrieve(external_id="Water_separated")
    create_timeseries(client, resp_pf, resp_op, resp_wp)

    if dps_len > 0:
        client.time_series.data.insert_dataframe(df_all, external_id_headers=True, dropna=True)
    print(f"{dps_len} datapoints written")
    return dps_len


# handle(data,client)
