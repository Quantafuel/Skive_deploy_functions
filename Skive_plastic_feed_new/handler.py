# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 11:23:27 2022

@author: SébastienPissot
"""

import numpy as np
import pandas as pd


# from cog_client import get_client
# from cognite.client.data_classes import TimeSeries


# doc:
# https://cognite-docs.readthedocs-hosted.com/projects/cognite-sdk-python/en/latest/

# client = get_client()

# Use the following if time series need to be deleted
# client.time_series.delete(external_id="Old_Plastic_feed", ignore_unknown_ids=True)


# %% Handle function
def handle(data, client):
    # print(data)
    data = {
        "agg": "average",
        "start_time": "2h-ago",
        "end_time": "now",
        "gran": "1m",
        "threshold_inscrew": 10,
        "threshold_Vpump": 3,
        "threshold_N2circ": 90,
        "threshold_cond_pump": 10,
        "threshold_fg_temp": 100,
        "ncg_density": 1.6,
        "NCGf": ["2s=P10_EKG_BF001_MFLW01:M_MID"],
        "NCGb1": ["2s=P01_EKG_BF604:M_MID"],
        "NCGb2": ["2s=P02_EKG_BF604:M_MID"],
        "NCGb3": ["2s=P03_EKG_BF604:M_MID"],
        "NCGb4": ["2s=P04_EKG_BF604:M_MID"],
        "LO_M1": ["2s=P11_EGG_BF924_MFLW01:M_MID"],
        "LO_M2": ["2s=P12_EGG_BF974_MFLW01:M_MID"],
        "HO1": ["2s=P01_EGG_BF102_MFLW01:M_MID"],
        "HO2": ["2s=P02_EGG_BF202_MFLW01:M_MID"],
        "HO3": ["2s=P03_EGG_BF302_MFLW01:M_MID"],
        "HO4": ["2s=P04_EGG_BF402_MFLW01:M_MID"],
        "HO_cond1": ["2s=P01_EGG_BF103_MFLW01:M_MID"],
        "HO_cond2": ["2s=P02_EGG_BF203_MFLW01:M_MID"],
        "HO_cond3": ["2s=P03_EGG_BF303_MFLW01:M_MID"],
        "HO_cond4": ["2s=P04_EGG_BF403_MFLW01:M_MID"],
        "W_M1": ["2s=P11_GNK_BF925_MFLW01:M_MID"],
        "W_M2": ["2s=P12_GNK_BF975_MFLW01:M_MID"],
        "Y3Fflow1": ["2s=P01_EGG_BF101_MFLW01:M_MID"],
        "Y3Fflow2": ["2s=P02_EGG_BF201_MFLW01:M_MID"],
        "Y3Fflow3": ["2s=P03_EGG_BF301_MFLW01:M_MID"],
        "Y3Fflow4": ["2s=P04_EGG_BF401_MFLW01:M_MID"],
        "YFeedIn1": ["2s=P01EAC01GL001M101:M_HAST"],
        "YFeedIn2": ["2s=P02EAC02GL001M201:M_HAST"],
        "YFeedIn3": ["2s=P03EAC03GL001M301:M_HAST"],
        "YFeedIn4": ["2s=P04EAC04GL001M401:M_HAST"],
        "Vpump1_old": ["2s=P01_EGG_QN120:M_POS"],
        "Vpump2_old": ["2s=P02_EGG_QN220:M_POS"],
        "Vpump3_old": ["2s=P03_EGG_QN320:M_POS"],
        "Vpump4_old": ["2s=P04_EGG_QN420:M_POS"],
        "Vpump1_new": ["2s=P01_EGG_QN120A:M_POS"],
        "Vpump2_new": ["2s=P02_EGG_QN220A:M_POS"],
        "Vpump3_new": ["2s=P03_EGG_QN320A:M_POS"],
        "Vpump4_new": ["2s=P04_EGG_QN420A:M_POS"],
        "N2circ1": ["2s=P01_QJB_QN152:M_POS"],
        "N2circ2": ["2s=P02_QJB_QN252:M_POS"],
        "N2circ3": ["2s=P03_QJB_QN352:M_POS"],
        "N2circ4": ["2s=P04_QJB_QN452:M_POS"],
        "cond_pump1": ["2s=P01_EGG_GP101:M_SPD"],
        "cond_pump2": ["2s=P02_EGG_GP201:M_SPD"],
        "cond_pump3": ["2s=P03_EGG_GP301:M_SPD"],
        "cond_pump4": ["2s=P04_EGG_GP401:M_SPD"],
        "YT_FGIN1": ["2s=P01_RAA_TRC101_102_MTMP01:M_MID"],
        "YT_FGIN2": ["2s=P02_RAA_TRC201_202_MTMP01:M_MID"],
        "YT_FGIN3": ["2s=P03_RAA_TRC301_302_MTMP01:M_MID"],
        "YT_FGIN4": ["2s=P04_RAA_TRC401_402_MTMP01:M_MID"],
    }

    # Data with step-interpolation aggregation
    all_ts_list = data["LO_M1"].copy()
    all_ts_list.extend(data["LO_M2"].copy())
    all_ts_list.extend(data["HO1"].copy())
    all_ts_list.extend(data["HO2"].copy())
    all_ts_list.extend(data["HO3"].copy())
    all_ts_list.extend(data["HO4"].copy())
    all_ts_list.extend(data["HO_cond1"].copy())
    all_ts_list.extend(data["HO_cond2"].copy())
    all_ts_list.extend(data["HO_cond3"].copy())
    all_ts_list.extend(data["HO_cond4"].copy())
    all_ts_list.extend(data["W_M1"].copy())
    all_ts_list.extend(data["W_M2"].copy())
    all_ts_list.extend(data["YFeedIn1"].copy())
    all_ts_list.extend(data["YFeedIn2"].copy())
    all_ts_list.extend(data["YFeedIn3"].copy())
    all_ts_list.extend(data["YFeedIn4"].copy())
    all_ts_list.extend(data["cond_pump1"].copy())
    all_ts_list.extend(data["cond_pump2"].copy())
    all_ts_list.extend(data["cond_pump3"].copy())
    all_ts_list.extend(data["cond_pump4"].copy())

    client.assets.list(limit=1)
    data_all = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=["stepInterpolation"],
        granularity=data["gran"],
    )

    # Retrieve latest datapoint if first datapoint of a timeseries is NaN
    for column in data_all.columns:
        if "new" in column:
            continue
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
    data_all = data_all.fillna(0)

    # Data with average aggregation
    all_ts_list2 = data["Vpump1_old"].copy()
    all_ts_list2.extend(data["Vpump2_old"].copy())
    all_ts_list2.extend(data["Vpump3_old"].copy())
    all_ts_list2.extend(data["Vpump4_old"].copy())
    all_ts_list2.extend(data["Vpump1_new"].copy())
    all_ts_list2.extend(data["Vpump2_new"].copy())
    all_ts_list2.extend(data["Vpump3_new"].copy())
    all_ts_list2.extend(data["Vpump4_new"].copy())
    all_ts_list2.extend(data["N2circ1"].copy())
    all_ts_list2.extend(data["N2circ2"].copy())
    all_ts_list2.extend(data["N2circ3"].copy())
    all_ts_list2.extend(data["N2circ4"].copy())
    all_ts_list2.extend(data["NCGf"].copy())
    all_ts_list2.extend(data["NCGb1"].copy())
    all_ts_list2.extend(data["NCGb2"].copy())
    all_ts_list2.extend(data["NCGb3"].copy())
    all_ts_list2.extend(data["NCGb4"].copy())
    all_ts_list2.extend(data["Y3Fflow1"].copy())
    all_ts_list2.extend(data["Y3Fflow2"].copy())
    all_ts_list2.extend(data["Y3Fflow3"].copy())
    all_ts_list2.extend(data["Y3Fflow4"].copy())
    all_ts_list2.extend(data["YT_FGIN1"].copy())
    all_ts_list2.extend(data["YT_FGIN2"].copy())
    all_ts_list2.extend(data["YT_FGIN3"].copy())
    all_ts_list2.extend(data["YT_FGIN4"].copy())

    client.assets.list(limit=1)
    data_all2 = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list2,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=["average"],
        granularity=data["gran"],
    )

    data_all = pd.concat([data_all, data_all2], axis="columns")

    data_all.columns = [
        "LO_M1",
        "LO_M2",
        "HO1",
        "HO2",
        "HO3",
        "HO4",
        "HO_cond1",
        "HO_cond2",
        "HO_cond3",
        "HO_cond4",
        "W_M1",
        "W_M2",
        "YFeedIn1",
        "YFeedIn2",
        "YFeedIn3",
        "YFeedIn4",
        "cond_pump1",
        "cond_pump2",
        "cond_pump3",
        "cond_pump4",
        "Vpump1_old",
        "Vpump2_old",
        "Vpump3_old",
        "Vpump4_old",
        "Vpump1_new",
        "Vpump2_new",
        "Vpump3_new",
        "Vpump4_new",
        "N2circ1",
        "N2circ2",
        "N2circ3",
        "N2circ4",
        "NCGf",
        "NCGb1",
        "NCGb2",
        "NCGb3",
        "NCGb4",
        "Y3Fflow1",
        "Y3Fflow2",
        "Y3Fflow3",
        "Y3Fflow4",
        "YT_FGIN1",
        "YT_FGIN2",
        "YT_FGIN3",
        "YT_FGIN4",
    ]

    data_all["Vpump1_old"] = data_all["Vpump1_old"].fillna(0)
    data_all["Vpump2_old"] = data_all["Vpump2_old"].fillna(0)
    data_all["Vpump3_old"] = data_all["Vpump3_old"].fillna(0)
    data_all["Vpump4_old"] = data_all["Vpump4_old"].fillna(0)

    # %% Making sure all flows are non-negative
    if data_all["Y3Fflow1"] < 0:
        data_all["Y3Fflow1"] = 0
    if data_all["Y3Fflow2"] < 0:
        data_all["Y3Fflow2"] = 0
    if data_all["Y3Fflow3"] < 0:
        data_all["Y3Fflow3"] = 0
    if data_all["Y3Fflow4"] < 0:
        data_all["Y3Fflow4"] = 0

    # Retrieve latest datapoint if first datapoint of a timeseries is NaN
    for column in data_all.columns:
        if "new" in column:
            continue
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

    data_all[data_all < 1] = 0  # Removing noise
    data_all = data_all.fillna(method="ffill")

    # Filter out wrong HO condensate values
    for lst in ["1", "2", "3", "4"]:
        data_all.loc[data_all["cond_pump" + lst] < data["threshold_cond_pump"], "HO_cond" + lst] = 0

    # merge Vacuum pump data
    data_all["Vpump1"] = data_all["Vpump1_old"] + data_all["Vpump1_new"]
    data_all["Vpump2"] = data_all["Vpump2_old"] + data_all["Vpump2_new"]
    data_all["Vpump3"] = data_all["Vpump3_old"] + data_all["Vpump3_new"]
    data_all["Vpump4"] = data_all["Vpump4_old"] + data_all["Vpump4_new"]

    # data_all=data_all.resample('1T').mean()
    # breakpoint()
    # %% Criteria to determine which lines are running
    #  Thresholds
    threshold_inscrew = data["threshold_inscrew"]  # Threshold for screw speed (%) to consider the line as working
    threshold_Vpump = data["threshold_Vpump"]  # %
    threshold_N2circ = data["threshold_N2circ"]  # %

    # %% Determine which lines are on based on the inscrew speed, and the gas and HO flow after flash tank
    Granularity = data["gran"]
    deltaT = pd.Timedelta(Granularity)
    data_all[["Status Line 1", "Status Line 2", "Status Line 3", "Status Line 4"]] = "Standby"

    for line in ["1", "2", "3", "4"]:
        index1 = data_all[data_all["YFeedIn" + line] > threshold_inscrew].index
        index2 = data_all[data_all["Vpump" + line] > threshold_Vpump].index
        data_all.loc[(data_all.index.isin(index1)) & (data_all.index.isin(index2)), "Status Line " + line] = "Running"

        data_all.loc[
            (data_all["Vpump" + line] <= threshold_Vpump) | (data_all["N2circ" + line] >= threshold_N2circ),
            "Y3Fflow" + line,
        ] = 0

        # last_run=pd.Timestamp(1998,1,1,0,0)
        # for dp in data_all.index:
        #     if data_all.loc[dp,'Status Line ' +line]=='Running':
        #         last_run=dp
        #         continue
        #     if dp-last_run>threshold_timesince:
        #         data_all.loc[dp,'Y3Fflow'  + line]=0

        # ----------------------------------------------------------------------
        # Remove 3P sep flow data where no data for HO or LO is available
        # since at least 15 min
        # ----------------------------------------------------------------------
        if deltaT < pd.Timedelta("15T"):
            if line == "1" or line == "2":
                mod = "1"
            elif line == "3" or line == "4":
                mod = "2"
            data_all["Last good " + line] = pd.NaT
            data_all.loc[(data_all["HO" + line] > 0), "Last good " + line] = data_all[(data_all["HO" + line] > 0)].index

            if data_all["Last good " + line].isna().all():
                data_all.loc[:, "Last good " + line] = pd.Timestamp(1998, 1, 1, 0, 0)
            elif data_all["Last good " + line][0] is pd.NaT:
                data_all.iloc[0, data_all.columns.get_indexer(["Last good " + line])] = pd.Timestamp(1998, 1, 1, 0, 0)

            data_all["Last good " + line] = data_all["Last good " + line].fillna(method="ffill")
            data_all.loc[
                ((data_all.index - data_all["Last good " + line]) > pd.Timedelta("15T"))
                & (data_all["Status Line " + line] == "Standby"),
                "Y3Fflow" + line,
            ] = 0

        # Add HO from condensate tank to HO values, first setting evert HO_cond value before Feb 25th 2022 to 0
        data_all.loc[
            (data_all.index - pd.Timestamp(year=2022, month=2, day=22, hour=0, minute=0)) < pd.Timedelta(0),
            "HO_cond" + line,
        ] = 0
        data_all["HO" + line] += data_all["HO_cond" + line]

    # NCG mass rate
    # Filtering NCG to flare
    data_all.loc[data_all["NCGf"] < 20, "NCGf"] = 0
    # Filtering NCG to burners
    for line in ["1", "2", "3", "4"]:
        data_all.loc[data_all["YT_FGIN" + line] < data["threshold_fg_temp"], "NCGb" + line] = 0

    # Calculating total NCG flow in kg/h
    data_all["NCG"] = data_all["NCGf"] + (data_all["NCGb1"] + data_all["NCGb2"] + data_all["NCGb3"] + data_all["NCGb4"])

    # Removing noisy NCG data
    # data_all['NCG'][data_all['NCG']<20]=0

    # %% Filtering out

    data_all_filt = data_all.copy(deep=True)
    data_all_filt.drop(
        data_all_filt[
            data_all_filt["Status Line 1"]
            + data_all_filt["Status Line 2"]
            + data_all_filt["Status Line 3"]
            + data_all_filt["Status Line 4"]
            == "StandbyStandbyStandbyStandby"
        ].index,
        axis="index",
        inplace=True,
    )

    for line in [1, 2, 3, 4]:
        data_all_filt.loc[data_all_filt["Status Line " + str(line)] == "Standby", "Y3Fflow" + str(line)] = 0
        data_all.loc[
            (data_all["Status Line " + str(line)] == "Standby") & (data_all["HO" + str(line)] < 1), "HO" + str(line)
        ] = 0
        data_all_filt.loc[
            (data_all_filt["Status Line " + str(line)] == "Standby") & (data_all_filt["HO" + str(line)] < 1),
            "HO" + str(line),
        ] = 0

    # %% Balance per line

    # Ratio of 3P flow to module per line
    Ratio1 = data_all["Y3Fflow1"] / (data_all["Y3Fflow1"] + data_all["Y3Fflow2"])
    Ratio2 = data_all["Y3Fflow2"] / (data_all["Y3Fflow1"] + data_all["Y3Fflow2"])
    Ratio3 = data_all["Y3Fflow3"] / (data_all["Y3Fflow3"] + data_all["Y3Fflow4"])
    Ratio4 = data_all["Y3Fflow4"] / (data_all["Y3Fflow3"] + data_all["Y3Fflow4"])
    Ratio = pd.concat([Ratio1, Ratio2, Ratio3, Ratio4], join="outer", axis=1)
    Ratio = Ratio.rename(columns={0: "Ratio Line 1", 1: "Ratio Line 2", 2: "Ratio Line 3", 3: "Ratio Line 4"})

    # Ratio of 3P flow over the four lines, for NCG distribution
    Ratio_NCG1 = data_all["Y3Fflow1"] / (
        data_all["Y3Fflow1"] + data_all["Y3Fflow2"] + data_all["Y3Fflow3"] + data_all["Y3Fflow4"]
    )
    Ratio_NCG2 = data_all["Y3Fflow2"] / (
        data_all["Y3Fflow1"] + data_all["Y3Fflow2"] + data_all["Y3Fflow3"] + data_all["Y3Fflow4"]
    )
    Ratio_NCG3 = data_all["Y3Fflow3"] / (
        data_all["Y3Fflow1"] + data_all["Y3Fflow2"] + data_all["Y3Fflow3"] + data_all["Y3Fflow4"]
    )
    Ratio_NCG4 = data_all["Y3Fflow4"] / (
        data_all["Y3Fflow1"] + data_all["Y3Fflow2"] + data_all["Y3Fflow3"] + data_all["Y3Fflow4"]
    )
    Ratio_NCG = pd.concat([Ratio_NCG1, Ratio_NCG2, Ratio_NCG3, Ratio_NCG4], join="outer", axis=1)
    Ratio_NCG = Ratio_NCG.rename(
        columns={0: "Ratio_NCG Line 1", 1: "Ratio_NCG Line 2", 2: "Ratio_NCG Line 3", 3: "Ratio_NCG Line 4"}
    )

    # In case the 3 phase flow measurement is wrong for a line, it will mess up the LO and/or NCG for the others
    # For such datapoint, use the ratio of screw speed - assuming single speed parameter for all lines
    idx1 = pd.Series(data_all[(data_all["Y3Fflow1"] <= 10) & (data_all["Status Line 1"] == "Running")].index)
    idx2 = pd.Series(data_all[(data_all["Y3Fflow2"] <= 10) & (data_all["Status Line 2"] == "Running")].index)
    idx3 = pd.Series(data_all[(data_all["Y3Fflow3"] <= 10) & (data_all["Status Line 3"] == "Running")].index)
    idx4 = pd.Series(data_all[(data_all["Y3Fflow4"] <= 10) & (data_all["Status Line 4"] == "Running")].index)
    idxM1 = pd.concat([idx1, idx2], axis="index")
    idxM1.drop_duplicates(inplace=True)
    idxM2 = pd.concat([idx3, idx4], axis="index")
    idxM2.drop_duplicates(inplace=True)
    idx = pd.concat([idx1, idx2, idx3, idx4], axis="index")
    idx.drop_duplicates(inplace=True)

    for line in ["1", "2", "3", "4"]:
        if (line == "1") or (line == "2"):
            Ratio.loc[Ratio.index.isin(idxM1), "Ratio Line " + line] = data_all["YFeedIn" + line][
                data_all.index.isin(idxM1)
            ] / (data_all["YFeedIn1"][data_all.index.isin(idxM1)] + data_all["YFeedIn2"][data_all.index.isin(idxM1)])
        if (line == "3") or (line == "4"):
            Ratio.loc[Ratio.index.isin(idxM2), "Ratio Line " + line] = data_all["YFeedIn" + line][
                data_all.index.isin(idxM2)
            ] / (data_all["YFeedIn3"][data_all.index.isin(idxM2)] + data_all["YFeedIn4"][data_all.index.isin(idxM2)])
        Ratio_NCG["Ratio_NCG Line " + line][Ratio_NCG.index.isin(idx)] = data_all["YFeedIn" + line][
            data_all.index.isin(idx)
        ] / (
            data_all["YFeedIn1"][data_all.index.isin(idx)]
            + data_all["YFeedIn2"][data_all.index.isin(idx)]
            + data_all["YFeedIn3"][data_all.index.isin(idx)]
            + data_all["YFeedIn4"][data_all.index.isin(idx)]
        )

    # Light Oil
    LO1 = Ratio["Ratio Line 1"].fillna(0) * data_all["LO_M1"]
    LO2 = Ratio["Ratio Line 2"].fillna(0) * data_all["LO_M1"]
    LO3 = Ratio["Ratio Line 3"].fillna(0) * data_all["LO_M2"]
    LO4 = Ratio["Ratio Line 4"].fillna(0) * data_all["LO_M2"]

    # Total Oil
    TO1 = LO1 + data_all["HO1"]
    TO2 = LO2 + data_all["HO2"]
    TO3 = LO3 + data_all["HO3"]
    TO4 = LO4 + data_all["HO4"]

    # Water
    W1 = Ratio["Ratio Line 1"].fillna(0) * data_all["W_M1"]
    W2 = Ratio["Ratio Line 2"].fillna(0) * data_all["W_M1"]
    W3 = Ratio["Ratio Line 3"].fillna(0) * data_all["W_M2"]
    W4 = Ratio["Ratio Line 4"].fillna(0) * data_all["W_M2"]

    # NCG
    NCG1 = (Ratio_NCG["Ratio_NCG Line 1"]).fillna(0) * data_all["NCG"]
    NCG2 = (Ratio_NCG["Ratio_NCG Line 2"]).fillna(0) * data_all["NCG"]
    NCG3 = (Ratio_NCG["Ratio_NCG Line 3"]).fillna(0) * data_all["NCG"]
    NCG4 = (Ratio_NCG["Ratio_NCG Line 4"]).fillna(0) * data_all["NCG"]

    # Plastic "Products" flow
    PF1_prod = (TO1 + NCG1) / 0.9 + W1
    PF2_prod = (TO2 + NCG2) / 0.9 + W2
    PF3_prod = (TO3 + NCG3) / 0.9 + W3
    PF4_prod = (TO4 + NCG4) / 0.9 + W4
    PF_prod_new = pd.concat([PF1_prod, PF2_prod, PF3_prod, PF4_prod], join="outer", axis=1)
    PF_prod_new = PF_prod_new.rename(columns={0: "Feed Line 1", 1: "Feed Line 2", 2: "Feed Line 3", 3: "Feed Line 4"})

    # Balance 3P sep
    Bal3P_M1 = (data_all["Y3Fflow1"] + data_all["Y3Fflow2"]) - (LO1 + LO2 + W1 + W2 + NCG1 + NCG2)
    Bal3P_M2 = (data_all["Y3Fflow3"] + data_all["Y3Fflow4"]) - (LO3 + LO4 + W3 + W4 + NCG3 + NCG4)
    Bal3P_M = pd.concat([Bal3P_M1, Bal3P_M2], join="outer", axis=1)
    Bal3P_M = Bal3P_M.rename(columns={0: "Bal3P_M1", 1: "Bal3P_M2"})

    In3P_M1 = data_all["Y3Fflow1"] + data_all["Y3Fflow2"]
    In3P_M2 = data_all["Y3Fflow3"] + data_all["Y3Fflow4"]
    In3P_M = pd.concat([In3P_M1, In3P_M2], join="outer", axis=1)
    In3P_M = In3P_M.rename(columns={0: "In3P_M1", 1: "In3P_M2"})

    Out3P_M1 = LO1 + LO2 + W1 + W2 + NCG1 + NCG2
    Out3P_M2 = LO3 + LO4 + W3 + W4 + NCG3 + NCG4
    Out3P_M = pd.concat([Out3P_M1, Out3P_M2], join="outer", axis=1)
    Out3P_M = Out3P_M.rename(columns={0: "Out3P_M1", 1: "Out3P_M2"})

    # Plastic flow based on 3PFlow -ACCOUNT FOR ASH IN DIFFERENT WAY!
    PF1_3PF = (data_all["Y3Fflow1"] + data_all["HO1"]) / 0.9
    PF2_3PF = (data_all["Y3Fflow2"] + data_all["HO2"]) / 0.9
    PF3_3PF = (data_all["Y3Fflow3"] + data_all["HO3"]) / 0.9
    PF4_3PF = (data_all["Y3Fflow4"] + data_all["HO4"]) / 0.9
    PF_3PF = pd.concat([PF1_3PF, PF2_3PF, PF3_3PF, PF4_3PF], join="outer", axis=1)
    PF_3PF = PF_3PF.rename(columns={0: "PF1_3PF", 1: "PF2_3PF", 2: "PF3_3PF", 3: "PF4_3PF"})

    # Filtered Plastic flow
    PF_filt = PF_prod_new.copy(deep=True)
    PF_filt = PF_filt.rename(
        columns={
            "Feed Line 1": "PF1_filt",
            "Feed Line 2": "PF2_filt",
            "Feed Line 3": "PF3_filt",
            "Feed Line 4": "PF4_filt",
        }
    )
    PF_filt.drop(index=PF_filt[~PF_filt.index.isin(data_all_filt.index)].index, inplace=True)
    for line in ["1", "2", "3", "4"]:
        PF_filt["PF" + line + "_filt"][data_all_filt["Status Line " + line] == "Standby"] = 0

    # PF old
    PF_old = (TO1 + TO2 + TO3 + TO4 + NCG1 + NCG2 + NCG3 + NCG4) / 0.9 + (W1 + W2 + W3 + W4)

    # Output DataFrame
    Output = pd.concat(
        [
            PF_filt["PF1_filt"],
            PF_filt["PF2_filt"],
            PF_filt["PF3_filt"],
            PF_filt["PF4_filt"],
            LO1,
            LO2,
            LO3,
            LO4,
            W1,
            W2,
            W3,
            W4,
            NCG1,
            NCG2,
            NCG3,
            NCG4,
            TO1,
            TO2,
            TO3,
            TO4,
            PF1_prod,
            PF2_prod,
            PF3_prod,
            PF4_prod,
            PF_old,
            data_all["NCGb1"] * data["ncg_density"],
            data_all["NCGb2"] * data["ncg_density"],
            data_all["NCGb3"],
            data_all["NCGb4"] * data["ncg_density"],
            data_all["NCGf"],
            PF_3PF["PF1_3PF"],
            PF_3PF["PF2_3PF"],
            PF_3PF["PF3_3PF"],
            PF_3PF["PF4_3PF"],
            Bal3P_M["Bal3P_M1"],
            Bal3P_M["Bal3P_M2"],
            In3P_M["In3P_M1"],
            In3P_M["In3P_M2"],
            Out3P_M["Out3P_M1"],
            Out3P_M["Out3P_M2"],
        ],
        join="outer",
        axis=1,
    )
    Output = Output.rename(
        columns={
            0: "LO1",
            1: "LO2",
            2: "LO3",
            3: "LO4",
            4: "W1",
            5: "W2",
            6: "W3",
            7: "W4",
            8: "NCG1",
            9: "NCG2",
            10: "NCG3",
            11: "NCG4",
            12: "TO1",
            13: "TO2",
            14: "TO3",
            15: "TO4",
            16: "PF1_prod",
            17: "PF2_prod",
            18: "PF3_prod",
            19: "PF4_prod",
            20: "PF_old",
            21: "NCGb1",
            22: "NCGb2",
            23: "NCGb3",
            24: "NCGb4",
            25: "NCGf",
            26: "PF1_3PF",
            27: "PF2_3PF",
            28: "PF3_3PF",
            29: "PF4_3PF",
            30: "Bal3P_M1",
            31: "Bal3P_M2",
            32: "In3P_M1",
            33: "In3P_M2",
            34: "Out3P_M1",
            35: "Out3P_M1",
        }
    )
    Output = Output.fillna(0)
    for line in [1, 2, 3, 4]:
        Output.loc[:, "Status_Line_" + str(line)] = 0
        Output.loc[data_all["Status Line " + str(line)] == "Running", "Status_Line_" + str(line)] = (
            1 * deltaT.seconds / 3600
        )
    Output = Output.resample("1T").mean()

    # %% Exporting time series to CDF - Taken from handle.py of the original feed calculation cognite function
    df_all = pd.DataFrame(index=Output.index)
    count = 0

    for line in [1, 2, 3, 4]:

        # Export dataframe
        df_all["New_Plastic_feed_" + str(line)] = Output["PF" + str(line) + "_filt"]
        df_all["New_Plastic_feed_tot_" + str(line)] = Output["PF" + str(line) + "_prod"]
        df_all["Plastic_feed_3PF_" + str(line)] = Output["PF" + str(line) + "_3PF"]
        df_all["New_Oil_produced_" + str(line)] = Output["TO" + str(line)]
        df_all["New_Water_separated_" + str(line)] = Output["W" + str(line)]
        df_all["New_NCG_produced_" + str(line)] = Output["NCG" + str(line)]
        # df_all['NCG_burner_' + str(line)]=Output['NCGb' + str(line)]
        df_all["Status_Line_" + str(line)] = Output["Status_Line_" + str(line)]
        # df_all['Status_Line_' + str(line)]=0
        # df_all.loc[data_all['Status Line ' + str(line)]=='Running', 'Status_Line_' + str(line)]=1*deltaT.seconds/3600
        dps_len = len(df_all)

        # =============================================================================
        #         resp_pf = client.time_series.retrieve(external_id="New_Plastic_feed_" + str(line))
        #         resp_pf_tot = client.time_series.retrieve(external_id="New_Plastic_feed_tot_" + str(line))
        #         resp_pf_3pf = client.time_series.retrieve(external_id="Plastic_feed_3PF_" + str(line))
        #         resp_op = client.time_series.retrieve(external_id="New_Oil_produced_" + str(line))
        #         resp_wp = client.time_series.retrieve(external_id="New_Water_separated_" + str(line))
        #         resp_ncg = client.time_series.retrieve(external_id="New_NCG_produced_" + str(line))
        #         resp_sta = client.time_series.retrieve(external_id="Status_Line_" + str(line))
        # =============================================================================
        # resp_ncgb = client.time_series.retrieve(external_id="NCG_burner_" + str(line))
        # create_timeseries(client, line, resp_pf, resp_pf_tot, resp_op, resp_wp, resp_ncg, resp_sta, resp_pf_3pf)

        count += dps_len * 8
        # if dps_len > 0:
        #     client.datapoints.insert_dataframe(df_all, external_id_headers=True, dropna=True)

    for mod in ["1", "2"]:
        df_all["Balance_3P_sep_M" + mod] = Output["Bal3P_M" + mod]
        df_all["Input_3P_sep_M" + mod] = Output["In3P_M" + mod]
        df_all["Output_3P_sep_M" + mod] = Output["Out3P_M" + mod]
        #        resp_bal = client.time_series.retrieve(external_id="Balance_3P_sep_M" + mod)
        #        resp_in = client.time_series.retrieve(external_id="Input_3P_sep_M" + mod)
        #        resp_out = client.time_series.retrieve(external_id="Output_3P_sep_M" + mod)
        # create_timeseries_mod(client, mod, resp_bal, resp_in, resp_out)
        count += dps_len * 3

    # Old calculation
    df_all["Old_Plastic_feed"] = Output["PF_old"]
    #    resp_pfold = client.time_series.retrieve(external_id="Old_Plastic_feed")
    # create_timeseries_pfold(client, resp_pfold)
    count += 1 * dps_len

    # NCG to flare
    df_all["NCG_flare"] = Output["NCGf"]
    #    resp_ncgf = client.time_series.retrieve(external_id="NCG_flare")
    #    create_timeseries_ncgf(client, resp_ncgf)
    count += 1 * dps_len

    if dps_len > 0:
        client.time_series.data.insert_dataframe(df_all, external_id_headers=True, dropna=True)

    print(f"{count} datapoints written")
    return count


# %% Time serie creation function
# =============================================================================
# def create_timeseries(client, line, pf, pf_tot, op, wp, ncg, sta, pf_3pf):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if pf is None:
#         ts1 = TimeSeries(
#             external_id="New_Plastic_feed_" + str(line),
#             name="New_Plastic_feed_" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Calculated based on products flow, weighed by the 3P flow. Filtered",
#         )
#         resp = client.time_series.create(ts1)
#     if op is None:
#         ts2 = TimeSeries(
#             external_id="New_Oil_produced_" + str(line),
#             name="New_Oil_produced_" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Calculated from the formula (Light_oil+Heavy_oil)",
#         )
#         resp = client.time_series.create(ts2)
#
#     if wp is None:
#         ts3 = TimeSeries(
#             external_id="New_Water_separated_" + str(line),
#             name="New_Water_separated_" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Water separated. Sum of 2s=P11_GNK_BF925_MFLW01:M_MID and '2s=P12_GNK_BF975_MFLW01:M_MID",
#         )
#         resp = client.time_series.create(ts3)
#
#     if ncg is None:
#         ts4 = TimeSeries(
#             external_id="New_NCG_produced_" + str(line),
#             name="New_NCG_produced_" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="NCG",
#         )
#         resp = client.time_series.create(ts4)
#     if pf_tot is None:
#         ts5 = TimeSeries(
#             external_id="New_Plastic_feed_tot_" + str(line),
#             name="New_Plastic_feed_tot_" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Plastic feed for total plastic fed calculation. Calculated based on products flow, weighed by the 3P flow",
#         )
#         resp = client.time_series.create(ts5)
#     if sta is None:
#         ts6 = TimeSeries(
#             external_id="Status_Line_" + str(line),
#             name="Status_Line_" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Line status: 0 = standby, 1 = running",
#         )
#         resp = client.time_series.create(ts6)
#
#     # if ncgb is None:
#     #     ts7 = TimeSeries(
#     #         external_id="NCG_burner_" + str(line),
#     #         name="NCG_burner_" + str(line),
#     #         metadata={"Info": "Calculated from cognite functions"},
#     #         asset_id=asset.id,
#     #         description="NCG to burner [kg/h], assuming density 1.6 kg/m3",
#     #     )
#     #     resp = client.time_series.create(ts7)
#
#     if pf_3pf is None:
#         ts8 = TimeSeries(
#             external_id="Plastic_feed_3PF_" + str(line),
#             name="Plastic_feed_3PF_" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Plastic feed based on 3P flow and HO+HHO. ACCOUNT FOR ASH IN DIFFERENT WAY!",
#         )
#         resp = client.time_series.create(ts8)
#
#
# def create_timeseries_mod(client, mod, bal, inl, out):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if bal is None:
#         ts1 = TimeSeries(
#             external_id="Balance_3P_sep_M" + mod,
#             name="Balance_3P_sep_M" + mod,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Balance over the 3 phases separator for module " + mod,
#         )
#         resp = client.time_series.create(ts1)
#
#     if inl is None:
#         ts2 = TimeSeries(
#             external_id="Input_3P_sep_M" + mod,
#             name="Input_3P_sep_M" + mod,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Input to the 3 phases separator for module " + mod,
#         )
#         resp = client.time_series.create(ts2)
#
#     if out is None:
#         ts3 = TimeSeries(
#             external_id="Output_3P_sep_M" + mod,
#             name="Output_3P_sep_M" + mod,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Output to the 3 phases separator for module " + mod,
#         )
#         resp = client.time_series.create(ts3)
#
#
# def create_timeseries_pfold(client, pfold):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if pfold is None:
#         ts = TimeSeries(
#             external_id="Old_Plastic_feed",
#             name="Old_Plastic_feed",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Old feed calculation",
#         )
#         resp = client.time_series.create(ts)
#
#
# def create_timeseries_ncgf(client, ncgf):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if ncgf is None:
#         ts = TimeSeries(
#             external_id="NCG_flare",
#             name="NCG_flare",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="NCG to flare [kg/h]",
#         )
#         resp = client.time_series.create(ts)
#
#
# =============================================================================
# handle(data, client)
