# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 08:38:39 2022

@author: SébastienPissot
"""


# %% Handle function
def handle(client):
    import numpy as np
    import pandas as pd

    data = {
        "agg": "stepInterpolation",
        "start_time": "121m-ago",
        "end_time": "now",
        "gran": "1m",
        "Pwdr1": ["2s=P01_EAC_BW101_MWGH01:M_MID"],
        "Pwdr2": ["2s=P02_EAC_BW201_MWGH01:M_MID"],
        "Pwdr3": ["2s=P03_EAC_BW301_MWGH01:M_MID"],
        "Pwdr4": ["2s=P04_EAC_BW401_MWGH01:M_MID"],
        "Speed1": ["2s=P01EAC01GL003M103:M_HAST"],
        "Speed2": ["2s=P02EAC02GL003M203:M_HAST"],
        "Speed3": ["2s=P03EAC03GL003M303:M_HAST"],
        "Speed4": ["2s=P04EAC04GL003M403:M_HAST"],
        "YFeedIn1": ["2s=P01EAC01GL001M101:M_HAST"],
        "YFeedIn2": ["2s=P02EAC02GL001M201:M_HAST"],
        "YFeedIn3": ["2s=P03EAC03GL001M301:M_HAST"],
        "YFeedIn4": ["2s=P04EAC04GL001M401:M_HAST"],
        "Cumul_Pwdr_1": ["Cumulative_Powder_1"],
        "Cumul_Pwdr_2": ["Cumulative_Powder_2"],
        "Cumul_Pwdr_3": ["Cumulative_Powder_3"],
        "Cumul_Pwdr_4": ["Cumulative_Powder_4"],
        "threshold": 1,
    }

    print(data)
    # Data with step-interpolation aggregation
    all_ts_list = data["Pwdr1"].copy()
    all_ts_list.extend(data["Pwdr2"].copy())
    all_ts_list.extend(data["Pwdr3"].copy())
    all_ts_list.extend(data["Pwdr4"].copy())
    all_ts_list.extend(data["Speed1"].copy())
    all_ts_list.extend(data["Speed2"].copy())
    all_ts_list.extend(data["Speed3"].copy())
    all_ts_list.extend(data["Speed4"].copy())
    all_ts_list.extend(data["YFeedIn1"].copy())
    all_ts_list.extend(data["YFeedIn2"].copy())
    all_ts_list.extend(data["YFeedIn3"].copy())
    all_ts_list.extend(data["YFeedIn4"].copy())
    all_ts_list.extend(data["Cumul_Pwdr_1"].copy())
    all_ts_list.extend(data["Cumul_Pwdr_2"].copy())
    all_ts_list.extend(data["Cumul_Pwdr_3"].copy())
    all_ts_list.extend(data["Cumul_Pwdr_4"].copy())

    client.assets.list(limit=1)
    data_all = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=[data["agg"]],
        granularity=data["gran"],
        ignore_unknown_ids=True,
    )

    for line in ["1", "2", "3", "4"]:
        if not data_all.columns.str.contains("Cumulative_Powder_" + line).any():
            data_all["Cumul_Pwdr_" + line] = 0

    data_all.columns = [
        "Pwdr1",
        "Pwdr2",
        "Pwdr3",
        "Pwdr4",
        "Speed1",
        "Speed2",
        "Speed3",
        "Speed4",
        "YFeedIn1",
        "YFeedIn2",
        "YFeedIn3",
        "YFeedIn4",
        "Cumul_Pwdr_1",
        "Cumul_Pwdr_2",
        "Cumul_Pwdr_3",
        "Cumul_Pwdr_4",
    ]

    # Retrieve latest datapoint if first datapoint of a timeseries is NaN
    for column in data_all.columns:
        if np.isnan(data_all[column][0]):
            try:
                data_all[column][0] = (
                    client.time_series.data.retrieve_latest(external_id=data[column], before=data["start_time"])
                    .to_pandas()
                    .iloc[0, 0]
                )
            except IndexError:
                # If no data before that date, set first value to 0
                data_all[column][0] = 0

    Granularity = data["gran"]
    deltaT = pd.Timedelta(Granularity).seconds / 3600

    for line in ["1", "2", "3", "4"]:

        # use only when getting historical data
        # data_all['Pwdr' + line][data_all.index<data_all['Pwdr' + line].first_valid_index()]=data_all['Pwdr' + line].loc[data_all['Pwdr' + line].first_valid_index()]

        data_all["Rate" + line] = -data_all["Pwdr" + line].diff() / deltaT
        data_all["Rate" + line][
            (data_all["Rate" + line] > -data["threshold"]) & (data_all["Rate" + line] < data["threshold"])
        ] = 0
        data_all["Rate" + line][data_all["YFeedIn" + line].fillna(method="ffill") < 5] = 0
        data_all["Rate" + line][data_all["Rate" + line] < 0] = np.nan
        data_all["Rate" + line][data_all["Rate" + line] > 50] = np.nan

        data_all["Cumul_Pwdr_" + line].iloc[1:] = data_all["Rate" + line].iloc[1:] * deltaT
        data_all["Cumul_Pwdr_" + line] = data_all["Cumul_Pwdr_" + line].cumsum()

    df_all = data_all.iloc[1:, 12:]
    df_all = df_all.fillna(method="ffill")
    df_all = df_all.rename(
        columns={
            "Cumul_Pwdr_1": "Cumulative_Powder_1",
            "Cumul_Pwdr_2": "Cumulative_Powder_2",
            "Cumul_Pwdr_3": "Cumulative_Powder_3",
            "Cumul_Pwdr_4": "Cumulative_Powder_4",
            "Rate1": "Powder_rate_1",
            "Rate2": "Powder_rate_2",
            "Rate3": "Powder_rate_3",
            "Rate4": "Powder_rate_4",
        }
    )

    # for line in ['1', '2', '3', '4']:

    # resp_pr = client.time_series.retrieve(external_id="Powder_rate_" + line,)
    # resp_cp = client.time_series.retrieve(external_id="Cumulative_Powder_" + line,)
    # create_timeseries_per_line(client, line, resp_pr, resp_cp)

    dps_len = len(df_all)
    if dps_len > 0:
        client.time_series.data.insert_dataframe(df_all, external_id_headers=True, dropna=True)

    count = dps_len * 8

    print(f"{count} datapoints written")
    return count
