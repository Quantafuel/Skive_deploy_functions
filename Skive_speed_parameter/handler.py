# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 11:23:27 2022

@author: SébastienPissot
"""


# doc:
# https://cognite-docs.readthedocs-hosted.com/projects/cognite-sdk-python/en/latest/

# client = get_client()


# data["start_time"]= "7d-ago"

# {"start_time": "4h-ago",
# "end_time": "now",
# "agg": "average",
# "gran": "1m",
# "input_timeseries": "Water_separated",
# "filter_type": "moving_average",
# "filter_order": 121}

# Use the following if time series need to be deleted
# if data["init"]==1:
#     for line in ['1', '2', '3', '4']:
#         client.time_series.delete(external_id="Exclude_from_SP_L" + line, ignore_unknown_ids=True)
#         client.time_series.delete(external_id="SpeedParam_Input_L" + line, ignore_unknown_ids=True)
#         client.time_series.delete(external_id="SpeedParam_Calc_" + line, ignore_unknown_ids=True)
#         client.time_series.delete(external_id="Cumulative_Screw_Feed_" + line, ignore_unknown_ids=True)


# %% Handle function
def handle(client):
    import numpy as np
    import pandas as pd

    data = {
        "agg": "step_interpolation",
        "start_time": "3h-ago",
        "end_time": "1h-ago",
        "gran": "1m",
        "stability_criterion": 30,
        "init": 0,
        "YFeedIn1": ["2s=P01EAC01GL001M101:M_HAST"],
        "YFeedIn2": ["2s=P02EAC02GL001M201:M_HAST"],
        "YFeedIn3": ["2s=P03EAC03GL001M301:M_HAST"],
        "YFeedIn4": ["2s=P04EAC04GL001M401:M_HAST"],
        "FeedSCADA1": ["2s=P01_INFD_PLAST_M_KG"],
        "FeedSCADA2": ["2s=P02_INFD_PLAST_M_KG"],
        "FeedSCADA3": ["2s=P03_INFD_PLAST_M_KG"],
        "FeedSCADA4": ["2s=P04_INFD_PLAST_M_KG"],
        "PF1": ["New_Plastic_feed_tot_1_filtered"],
        "PF2": ["New_Plastic_feed_tot_2_filtered"],
        "PF3": ["New_Plastic_feed_tot_3_filtered"],
        "PF4": ["New_Plastic_feed_tot_4_filtered"],
        "Exclude_from_SP_L1": ["Exclude_from_SP_L1"],
        "Exclude_from_SP_L2": ["Exclude_from_SP_L2"],
        "Exclude_from_SP_L3": ["Exclude_from_SP_L3"],
        "Exclude_from_SP_L4": ["Exclude_from_SP_L4"],
        "SP_input_L1": ["SpeedParam_Input_L1"],
        "SP_input_L2": ["SpeedParam_Input_L2"],
        "SP_input_L3": ["SpeedParam_Input_L3"],
        "SP_input_L4": ["SpeedParam_Input_L4"],
        "SP_calc1": ["SpeedParam_Calc_1"],
        "SP_calc2": ["SpeedParam_Calc_2"],
        "SP_calc3": ["SpeedParam_Calc_3"],
        "SP_calc4": ["SpeedParam_Calc_4"],
        "Cumul_Screw1": ["Cumulative_Screw_Feed_1"],
        "Cumul_Screw2": ["Cumulative_Screw_Feed_2"],
        "Cumul_Screw3": ["Cumulative_Screw_Feed_3"],
        "Cumul_Screw4": ["Cumulative_Screw_Feed_4"],
    }

    print(data)

    # Data with step-interpolation aggregation
    all_ts_list = data["YFeedIn1"].copy()
    all_ts_list.extend(data["YFeedIn2"].copy())
    all_ts_list.extend(data["YFeedIn3"].copy())
    all_ts_list.extend(data["YFeedIn4"].copy())
    all_ts_list.extend(data["FeedSCADA1"].copy())
    all_ts_list.extend(data["FeedSCADA2"].copy())
    all_ts_list.extend(data["FeedSCADA3"].copy())
    all_ts_list.extend(data["FeedSCADA4"].copy())
    all_ts_list.extend(data["PF1"].copy())
    all_ts_list.extend(data["PF2"].copy())
    all_ts_list.extend(data["PF3"].copy())
    all_ts_list.extend(data["PF4"].copy())
    all_ts_list.extend(data["Exclude_from_SP_L1"].copy())
    all_ts_list.extend(data["Exclude_from_SP_L2"].copy())
    all_ts_list.extend(data["Exclude_from_SP_L3"].copy())
    all_ts_list.extend(data["Exclude_from_SP_L4"].copy())
    all_ts_list.extend(data["SP_input_L1"].copy())
    all_ts_list.extend(data["SP_input_L2"].copy())
    all_ts_list.extend(data["SP_input_L3"].copy())
    all_ts_list.extend(data["SP_input_L4"].copy())
    all_ts_list.extend(data["SP_calc1"].copy())
    all_ts_list.extend(data["SP_calc2"].copy())
    all_ts_list.extend(data["SP_calc3"].copy())
    all_ts_list.extend(data["SP_calc4"].copy())
    all_ts_list.extend(data["Cumul_Screw1"].copy())
    all_ts_list.extend(data["Cumul_Screw2"].copy())
    all_ts_list.extend(data["Cumul_Screw3"].copy())
    all_ts_list.extend(data["Cumul_Screw4"].copy())

    # Parameter to initialize data - 0 to turn off
    if data["init"] == 1:
        data["start_time"] = pd.Timestamp(year=2023, month=2, day=14, hour=7)

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
        if not data_all.columns.str.contains("Exclude_from_SP_L" + line).any():
            data_all["Exclude_from_SP_L" + line] = 0
        if not data_all.columns.str.contains("SpeedParam_Input_L" + line).any():
            data_all["SP_input_L" + line] = 13
        if not data_all.columns.str.contains("SpeedParam_Calc_" + line).any():
            data_all["SP_calc" + line] = 0
        if not data_all.columns.str.contains("Cumulative_Screw_Feed_" + line).any():
            data_all["Cumul_Screw" + line] = 0

    dict_name = {
        "2s=P01EAC01GL001M101:M_HAST|" + data["agg"]: "YFeedIn1",
        "2s=P02EAC02GL001M201:M_HAST|" + data["agg"]: "YFeedIn2",
        "2s=P03EAC03GL001M301:M_HAST|" + data["agg"]: "YFeedIn3",
        "2s=P04EAC04GL001M401:M_HAST|" + data["agg"]: "YFeedIn4",
        "2s=P01_INFD_PLAST_M_KG|" + data["agg"]: "FeedSCADA1",
        "2s=P02_INFD_PLAST_M_KG|" + data["agg"]: "FeedSCADA2",
        "2s=P03_INFD_PLAST_M_KG|" + data["agg"]: "FeedSCADA3",
        "2s=P04_INFD_PLAST_M_KG|" + data["agg"]: "FeedSCADA4",
        "New_Plastic_feed_tot_1_filtered|" + data["agg"]: "PF1",
        "New_Plastic_feed_tot_2_filtered|" + data["agg"]: "PF2",
        "New_Plastic_feed_tot_3_filtered|" + data["agg"]: "PF3",
        "New_Plastic_feed_tot_4_filtered|" + data["agg"]: "PF4",
        "Exclude_from_SP_L1|" + data["agg"]: "Exclude_from_SP_L1",
        "Exclude_from_SP_L2|" + data["agg"]: "Exclude_from_SP_L2",
        "Exclude_from_SP_L3|" + data["agg"]: "Exclude_from_SP_L3",
        "Exclude_from_SP_L4|" + data["agg"]: "Exclude_from_SP_L4",
        "SpeedParam_Input_L1|" + data["agg"]: "SP_input_L1",
        "SpeedParam_Input_L2|" + data["agg"]: "SP_input_L2",
        "SpeedParam_Input_L3|" + data["agg"]: "SP_input_L3",
        "SpeedParam_Input_L4|" + data["agg"]: "SP_input_L4",
        "SpeedParam_Calc_1|" + data["agg"]: "SP_calc1",
        "SpeedParam_Calc_2|" + data["agg"]: "SP_calc2",
        "SpeedParam_Calc_3|" + data["agg"]: "SP_calc3",
        "SpeedParam_Calc_4|" + data["agg"]: "SP_calc4",
        "Cumulative_Screw_Feed_1|" + data["agg"]: "Cumul_Screw1",
        "Cumulative_Screw_Feed_2|" + data["agg"]: "Cumul_Screw2",
        "Cumulative_Screw_Feed_3|" + data["agg"]: "Cumul_Screw3",
        "Cumulative_Screw_Feed_4|" + data["agg"]: "Cumul_Screw4",
    }

    data_all = data_all.rename(columns=dict_name)

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

    # %% Indices (Timestamp) of the half of the data to fill
    indices = data_all.iloc[len(data_all) // 2 :].index
    indices2 = data_all.iloc[len(data_all) // 2 - 1 :].index

    # %% Input speed parameter
    for line in ["1", "2", "3", "4"]:
        data_all["SP_input_L" + line][data_all.index.isin(indices)] = (
            data_all["FeedSCADA" + line][data_all.index.isin(indices)]
        ) / (data_all["YFeedIn" + line][data_all.index.isin(indices)])
        data_all["SP_input_L" + line][(data_all["YFeedIn" + line] < 5) & (data_all.index.isin(indices))] = np.nan
        # data_all['SP_input_L' + line][data_all.index.isin(indices2)]=data_all['SP_input_L' + line][data_all.index.isin(indices2)].fillna(method='ffill')
        # data_all['SP_input_L' + line][data_all.index.isin(indices2)]=data_all['SP_input_L' + line][data_all.index.isin(indices2)].fillna(method='bfill')

    data_all = data_all.fillna(method="ffill")

    # %% Calculated speed parameter

    if data["init"] == 1:
        # Initiating the data
        init1 = data["start_time"]
        init2 = data["start_time"] + pd.Timedelta("1d")
        idx = data_all.index.get_loc(init2)

        for line in ["1", "2", "3", "4"]:
            data_all["Exclude_from_SP_L" + line][(data_all.index >= init1) & (data_all.index < init2)] = 1
            data_all["Exclude_from_SP_L" + line][(data_all.index >= init2) & (data_all["YFeedIn" + line] < 5)] = 1

            for i in range(idx, len(data_all) - 1):
                if (
                    data_all["Exclude_from_SP_L" + line].iloc[i] == 0
                    and data_all.iloc[i - 1]["Exclude_from_SP_L" + line] == 1
                ):
                    data_all["Exclude_from_SP_L" + line].iloc[
                        i - data["stability_criterion"] : i + data["stability_criterion"]
                    ] = 2
            data_all["Exclude_from_SP_L" + line][data_all["Exclude_from_SP_L" + line] == 2] = 1

    # Defining "stable operation"
    elif data["init"] == 0:
        for line in ["1", "2", "3", "4"]:
            data_all["Exclude_from_SP_L" + line] = 0
            data_all["Exclude_from_SP_L" + line][data_all["YFeedIn" + line] < 5] = 1
            for i in range(len(data_all) // 2, len(data_all) - 1):
                if (
                    data_all["Exclude_from_SP_L" + line].iloc[i] == 0
                    and data_all.iloc[i - 1]["Exclude_from_SP_L" + line] == 1
                ):
                    data_all["Exclude_from_SP_L" + line].iloc[
                        i - data["stability_criterion"] : i + data["stability_criterion"]
                    ] = 2
            data_all["Exclude_from_SP_L" + line][data_all["Exclude_from_SP_L" + line] == 2] = 1

    for line in ["1", "2", "3", "4"]:
        data_all["SP_calc" + line][data_all.index.isin(indices)] = (
            data_all["PF" + line][data_all.index.isin(indices)]
            / data_all["YFeedIn" + line][data_all.index.isin(indices)]
        )
        data_all["SP_calc" + line][data_all["Exclude_from_SP_L" + line] == 1] = 0
        data_all["SP_calc" + line][data_all["SP_calc" + line] < 5] = 0

    # %% Cumulative screw

    deltaT = pd.Timedelta(data["gran"])
    for line in ["1", "2", "3", "4"]:
        if data["init"] == 1:
            data_all["Cumul_Screw" + line][(data_all.index >= init1) & (data_all.index < init2)] = 0
            data_all["Cumul_Screw" + line][data_all.index >= init2] = (
                data_all["YFeedIn" + line][data_all.index >= init2] * deltaT.seconds / 3600
            )
            data_all["Cumul_Screw" + line][data_all.index >= init2] = data_all["Cumul_Screw" + line][
                data_all.index >= init2
            ].fillna(0)
            data_all["Cumul_Screw" + line] = data_all["Cumul_Screw" + line].cumsum()
        elif data["init"] == 0:
            data_all["Cumul_Screw" + line][data_all.index.isin(indices)] = (
                data_all["YFeedIn" + line][data_all.index.isin(indices)] * deltaT.seconds / 3600
            )
            data_all["Cumul_Screw" + line][data_all.index.isin(indices)] = data_all["Cumul_Screw" + line][
                data_all.index.isin(indices)
            ].fillna(0)
            data_all["Cumul_Screw" + line][data_all.index.isin(indices2)] = data_all["Cumul_Screw" + line][
                data_all.index.isin(indices2)
            ].cumsum()

    # %% Exporting time series to CDF - Taken from handle.py of the original feed calculation cognite function
    df_all = pd.DataFrame(index=data_all.index)
    count = 0

    # Export dataframe
    df_all["SpeedParam_Input_L1"] = data_all["SP_input_L1"]
    df_all["SpeedParam_Input_L2"] = data_all["SP_input_L2"]
    df_all["SpeedParam_Input_L3"] = data_all["SP_input_L3"]
    df_all["SpeedParam_Input_L4"] = data_all["SP_input_L4"]
    df_all["SpeedParam_Calc_1"] = data_all["SP_calc1"]
    df_all["SpeedParam_Calc_2"] = data_all["SP_calc2"]
    df_all["SpeedParam_Calc_3"] = data_all["SP_calc3"]
    df_all["SpeedParam_Calc_4"] = data_all["SP_calc4"]
    df_all["Exclude_from_SP_L1"] = data_all["Exclude_from_SP_L1"]
    df_all["Exclude_from_SP_L2"] = data_all["Exclude_from_SP_L2"]
    df_all["Exclude_from_SP_L3"] = data_all["Exclude_from_SP_L3"]
    df_all["Exclude_from_SP_L4"] = data_all["Exclude_from_SP_L4"]
    df_all["Cumulative_Screw_Feed_1"] = data_all["Cumul_Screw1"]
    df_all["Cumulative_Screw_Feed_2"] = data_all["Cumul_Screw2"]
    df_all["Cumulative_Screw_Feed_3"] = data_all["Cumul_Screw3"]
    df_all["Cumulative_Screw_Feed_4"] = data_all["Cumul_Screw4"]

    if data["init"] == 0:
        df_all = df_all[df_all.index.isin(indices)]

    dps_len = len(df_all)

    for line in ["1", "2", "3", "4"]:
        # resp_spi = client.time_series.retrieve(external_id="SpeedParam_Input_L" + line)
        # resp_spc = client.time_series.retrieve(external_id="SpeedParam_Calc_" + line)
        # resp_exc = client.time_series.retrieve(external_id="Exclude_from_SP_L" + line)
        # resp_cumu = client.time_series.retrieve(external_id="Cumulative_Screw_Feed_" + line)
        # create_timeseries_line(client, resp_spi, resp_spc, resp_exc, resp_cumu, line)
        count += 4 * dps_len

    if dps_len > 0:
        client.time_series.data.insert_dataframe(df_all, external_id_headers=True, dropna=True)

    print(f"{count} datapoints written")
    return count


# %% Time serie creation function
# def create_timeseries_line(client, spi, spc, exc, cumu, line):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if spc is None:
#         ts1 = TimeSeries(
#             external_id="SpeedParam_Calc_" + line,
#             name="SpeedParam_Calc_" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Calculated speed parameter per line",
#         )
#         resp = client.time_series.create(ts1)

#     if exc is None:
#         ts2 = TimeSeries(
#             external_id="Exclude_from_SP_L" + line,
#             name="Exclude_from_SP_L" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Parameter to know whether to exclude point from Speed Parameter calculation",
#         )
#         resp = client.time_series.create(ts2)

#     if spi is None:
#         ts3 = TimeSeries(
#             external_id="SpeedParam_Input_L" + line,
#             name="SpeedParam_Input_L" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Input speed parameter from SCADA for Line " + line,
#         )
#         resp = client.time_series.create(ts3)

#     if cumu is None:
#         ts3 = TimeSeries(
#             external_id="Cumulative_Screw_Feed_" + line,
#             name="Cumulative_Screw_Feed_" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative screw speed for Line " + line,
#         )
#         resp = client.time_series.create(ts3)

# # handle(data, client)
