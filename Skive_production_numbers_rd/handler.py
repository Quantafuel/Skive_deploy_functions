# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 11:23:27 2022

@author: SébastienPissot
"""


# %% Handle function
def handle(client):
    import numpy as np
    import pandas as pd

    data = {
        "agg1": "stepInterpolation",
        "agg2": "average",
        "start_time": "121m-ago",
        "end_time": "now",
        "gran": "1m",
        "elCO2": 226,
        "threshold_inscrew": 10,
        "threshold_Vpump": 3,
        "threshold_N2circ": 90,
        "threshold_cond_pump": 10,
        "NCG": ["2s=P10_EKG_BF001_MFLW01:M_MID"],
        "LO_M1": ["2s=P11_EGG_BF924_MFLW01:M_MID"],
        "LO_M2": ["2s=P12_EGG_BF974_MFLW01:M_MID"],
        "HO1": ["2s=P01_EGG_BF102_MFLW01:M_MID"],
        "HO2": ["2s=P02_EGG_BF202_MFLW01:M_MID"],
        "HO3": ["2s=P03_EGG_BF302_MFLW01:M_MID"],
        "HO4": ["2s=P04_EGG_BF402_MFLW01:M_MID"],
        "CO1": ["2s=P01_EGG_BF103_MFLW01:M_MID"],
        "CO2": ["2s=P02_EGG_BF203_MFLW01:M_MID"],
        "CO3": ["2s=P03_EGG_BF303_MFLW01:M_MID"],
        "CO4": ["2s=P04_EGG_BF403_MFLW01:M_MID"],
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
        "PF_tot_1": ["New_Plastic_feed_tot_1"],
        "PF_tot_2": ["New_Plastic_feed_tot_2"],
        "PF_tot_3": ["New_Plastic_feed_tot_3"],
        "PF_tot_4": ["New_Plastic_feed_tot_4"],
        "PF_old": ["Old_Plastic_feed"],
        "Vpump1_old": ["2s=P01_EGG_QN120:M_POS"],
        "Vpump2_old": ["2s=P02_EGG_QN220:M_POS"],
        "Vpump3_old": ["2s=P03_EGG_QN320:M_POS"],
        "Vpump4_old": ["2s=P04_EGG_QN420:M_POS"],
        "Vpump1_new": ["2s=P01_EGG_QN120A:M_POS"],
        "Vpump2_new": ["2s=P02_EGG_QN220A:M_POS"],
        "Vpump3_new": ["2s=P03_EGG_QN320A:M_POS"],
        "Vpump4_new": ["2s=P04_EGG_QN420A:M_POS"],
        "Cumul_feed_1": ["Cumulative_feed_1"],
        "Cumul_feed_2": ["Cumulative_feed_2"],
        "Cumul_feed_3": ["Cumulative_feed_3"],
        "Cumul_feed_4": ["Cumulative_feed_4"],
        "Cumul_feed_tot_prod": ["Cumulative_total_feed_products"],
        "Cumul_HO_1": ["Cumulative_flash_tank_oil_1"],
        "Cumul_HO_2": ["Cumulative_flash_tank_oil_2"],
        "Cumul_HO_3": ["Cumulative_flash_tank_oil_3"],
        "Cumul_HO_4": ["Cumulative_flash_tank_oil_4"],
        "Cumul_CO_1": ["Cumulative_condensate_oil_1"],
        "Cumul_CO_2": ["Cumulative_condensate_oil_2"],
        "Cumul_CO_3": ["Cumulative_condensate_oil_3"],
        "Cumul_CO_4": ["Cumulative_condensate_oil_4"],
        "Cumul_tot_Oil": ["Cumulative_total_Oil"],
        "Cumul_runt_1": ["Cumulative_operating_hours_1"],
        "Cumul_runt_2": ["Cumulative_operating_hours_2"],
        "Cumul_runt_3": ["Cumulative_operating_hours_3"],
        "Cumul_runt_4": ["Cumulative_operating_hours_4"],
        "Cumul_LO_M1": ["Cumulative_light_oil_M1"],
        "Cumul_LO_M2": ["Cumulative_light_oil_M2"],
        "Cumul_W_M1": ["Cumulative_water_M1"],
        "Cumul_W_M2": ["Cumulative_water_M2"],
        "Cumul_NCG": ["Cumulative_ncg"],
        "NGb1": ["2s=P01_QJD_BF603:M_MID"],
        "NGb2": ["2s=P02_QJD_BF603:M_MID"],
        "NGb3": ["2s=P03_QJD_BF603:M_MID"],
        "NGb4": ["2s=P04_QJD_BF603:M_MID"],
        "NGf": ["2s=P10_QJD_BF601:M_MID"],
        "NCGb1": ["2s=P01_EKG_BF604:M_MID"],
        "NCGb2": ["2s=P02_EKG_BF604:M_MID"],
        "NCGb3": ["2s=P03_EKG_BF604:M_MID"],
        "NCGb4": ["2s=P04_EKG_BF604:M_MID"],
        "Cumul_NGb_1": ["Cumulative_NG_burner_1"],
        "Cumul_NGb_2": ["Cumulative_NG_burner_2"],
        "Cumul_NGb_3": ["Cumulative_NG_burner_3"],
        "Cumul_NGb_4": ["Cumulative_NG_burner_4"],
        "Cumul_NG_flare": ["Cumulative_NG_flare"],
        "Cumul_NCGb_1": ["Cumulative_NCG_burner_1"],
        "Cumul_NCGb_2": ["Cumulative_NCG_burner_2"],
        "Cumul_NCGb_3": ["Cumulative_NCG_burner_3"],
        "Cumul_NCGb_4": ["Cumulative_NCG_burner_4"],
        "N2circ1": ["2s=P01_QJB_QN152:M_POS"],
        "N2circ2": ["2s=P02_QJB_QN252:M_POS"],
        "N2circ3": ["2s=P03_QJB_QN352:M_POS"],
        "N2circ4": ["2s=P04_QJB_QN452:M_POS"],
        "cond_pump1": ["2s=P01_EGG_GP101:M_SPD"],
        "cond_pump2": ["2s=P02_EGG_GP201:M_SPD"],
        "cond_pump3": ["2s=P03_EGG_GP301:M_SPD"],
        "cond_pump4": ["2s=P04_EGG_GP401:M_SPD"],
        "PMainboard": ["2s=P10_AAW01_BJ01:M_P"],
        "Cumul_PMainboard": ["Cumulative_El_consumption"],
        "Cumul_NCG_CO2": ["Cumulative_NCG_CO2"],
        "Cumul_NG_CO2": ["Cumulative_NG_CO2"],
        "Cumul_El_CO2": ["Cumulative_El_CO2"],
        "Cumul_avoid_CO2": ["Cumulative_avoided_CO2"],
    }

    print(data)
    # Data with step-interpolation aggregation
    all_ts_list = data["LO_M1"].copy()
    all_ts_list.extend(data["LO_M2"].copy())
    all_ts_list.extend(data["HO1"].copy())
    all_ts_list.extend(data["HO2"].copy())
    all_ts_list.extend(data["HO3"].copy())
    all_ts_list.extend(data["HO4"].copy())
    all_ts_list.extend(data["CO1"].copy())
    all_ts_list.extend(data["CO2"].copy())
    all_ts_list.extend(data["CO3"].copy())
    all_ts_list.extend(data["CO4"].copy())
    all_ts_list.extend(data["W_M1"].copy())
    all_ts_list.extend(data["W_M2"].copy())
    all_ts_list.extend(data["YFeedIn1"].copy())
    all_ts_list.extend(data["YFeedIn2"].copy())
    all_ts_list.extend(data["YFeedIn3"].copy())
    all_ts_list.extend(data["YFeedIn4"].copy())
    all_ts_list.extend(data["Vpump1_old"].copy())
    all_ts_list.extend(data["Vpump2_old"].copy())
    all_ts_list.extend(data["Vpump3_old"].copy())
    all_ts_list.extend(data["Vpump4_old"].copy())
    all_ts_list.extend(data["Vpump1_new"].copy())
    all_ts_list.extend(data["Vpump2_new"].copy())
    all_ts_list.extend(data["Vpump3_new"].copy())
    all_ts_list.extend(data["Vpump4_new"].copy())
    all_ts_list.extend(data["N2circ1"].copy())
    all_ts_list.extend(data["N2circ2"].copy())
    all_ts_list.extend(data["N2circ3"].copy())
    all_ts_list.extend(data["N2circ4"].copy())
    all_ts_list.extend(data["cond_pump1"].copy())
    all_ts_list.extend(data["cond_pump2"].copy())
    all_ts_list.extend(data["cond_pump3"].copy())
    all_ts_list.extend(data["cond_pump4"].copy())
    all_ts_list.extend(data["NGf"].copy())

    # Cumulative timeseries
    all_ts_list.extend(data["Cumul_feed_1"].copy())
    all_ts_list.extend(data["Cumul_feed_2"].copy())
    all_ts_list.extend(data["Cumul_feed_3"].copy())
    all_ts_list.extend(data["Cumul_feed_4"].copy())
    all_ts_list.extend(data["Cumul_feed_tot_prod"].copy())
    all_ts_list.extend(data["Cumul_HO_1"].copy())
    all_ts_list.extend(data["Cumul_HO_2"].copy())
    all_ts_list.extend(data["Cumul_HO_3"].copy())
    all_ts_list.extend(data["Cumul_HO_4"].copy())
    all_ts_list.extend(data["Cumul_CO_1"].copy())
    all_ts_list.extend(data["Cumul_CO_2"].copy())
    all_ts_list.extend(data["Cumul_CO_3"].copy())
    all_ts_list.extend(data["Cumul_CO_4"].copy())
    all_ts_list.extend(data["Cumul_LO_M1"].copy())
    all_ts_list.extend(data["Cumul_LO_M2"].copy())
    all_ts_list.extend(data["Cumul_tot_Oil"].copy())
    all_ts_list.extend(data["Cumul_W_M1"].copy())
    all_ts_list.extend(data["Cumul_W_M2"].copy())
    all_ts_list.extend(data["Cumul_NCG"].copy())
    all_ts_list.extend(data["Cumul_runt_1"].copy())
    all_ts_list.extend(data["Cumul_runt_2"].copy())
    all_ts_list.extend(data["Cumul_runt_3"].copy())
    all_ts_list.extend(data["Cumul_runt_4"].copy())
    all_ts_list.extend(data["Cumul_NGb_1"].copy())
    all_ts_list.extend(data["Cumul_NGb_2"].copy())
    all_ts_list.extend(data["Cumul_NGb_3"].copy())
    all_ts_list.extend(data["Cumul_NGb_4"].copy())
    all_ts_list.extend(data["Cumul_NG_flare"].copy())
    all_ts_list.extend(data["Cumul_NCGb_1"].copy())
    all_ts_list.extend(data["Cumul_NCGb_2"].copy())
    all_ts_list.extend(data["Cumul_NCGb_3"].copy())
    all_ts_list.extend(data["Cumul_NCGb_4"].copy())
    all_ts_list.extend(data["Cumul_PMainboard"].copy())
    all_ts_list.extend(data["Cumul_NCG_CO2"].copy())
    all_ts_list.extend(data["Cumul_NG_CO2"].copy())
    all_ts_list.extend(data["Cumul_El_CO2"].copy())
    all_ts_list.extend(data["Cumul_avoid_CO2"].copy())

    client.assets.list(limit=1)
    data_all = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=[data["agg1"]],
        granularity=data["gran"],
    )

    # Data with average aggregation
    all_ts_list_avg = data["NCG"].copy()
    all_ts_list_avg.extend(data["Y3Fflow1"].copy())
    all_ts_list_avg.extend(data["Y3Fflow2"].copy())
    all_ts_list_avg.extend(data["Y3Fflow3"].copy())
    all_ts_list_avg.extend(data["Y3Fflow4"].copy())
    all_ts_list_avg.extend(data["PF_tot_1"].copy())
    all_ts_list_avg.extend(data["PF_tot_2"].copy())
    all_ts_list_avg.extend(data["PF_tot_3"].copy())
    all_ts_list_avg.extend(data["PF_tot_4"].copy())
    all_ts_list_avg.extend(data["PF_old"].copy())
    all_ts_list_avg.extend(data["NGb1"].copy())
    all_ts_list_avg.extend(data["NGb2"].copy())
    all_ts_list_avg.extend(data["NGb3"].copy())
    all_ts_list_avg.extend(data["NGb4"].copy())
    all_ts_list_avg.extend(data["NCGb1"].copy())
    all_ts_list_avg.extend(data["NCGb2"].copy())
    all_ts_list_avg.extend(data["NCGb3"].copy())
    all_ts_list_avg.extend(data["NCGb4"].copy())
    all_ts_list_avg.extend(data["PMainboard"].copy())

    data_all2 = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list_avg,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=[data["agg2"]],
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
        "CO1",
        "CO2",
        "CO3",
        "CO4",
        "W_M1",
        "W_M2",
        "YFeedIn1",
        "YFeedIn2",
        "YFeedIn3",
        "YFeedIn4",
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
        "cond_pump1",
        "cond_pump2",
        "cond_pump3",
        "cond_pump4",
        "NGf",
        "Cumul_feed_1",
        "Cumul_feed_2",
        "Cumul_feed_3",
        "Cumul_feed_4",
        "Cumul_feed_tot_prod",
        "Cumul_HO_1",
        "Cumul_HO_2",
        "Cumul_HO_3",
        "Cumul_HO_4",
        "Cumul_CO_1",
        "Cumul_CO_2",
        "Cumul_CO_3",
        "Cumul_CO_4",
        "Cumul_LO_M1",
        "Cumul_LO_M2",
        "Cumul_tot_Oil",
        "Cumul_W_M1",
        "Cumul_W_M2",
        "Cumul_NCG",
        "Cumul_runt_1",
        "Cumul_runt_2",
        "Cumul_runt_3",
        "Cumul_runt_4",
        "Cumul_NGb_1",
        "Cumul_NGb_2",
        "Cumul_NGb_3",
        "Cumul_NGb_4",
        "Cumul_NG_flare",
        "Cumul_NCGb_1",
        "Cumul_NCGb_2",
        "Cumul_NCGb_3",
        "Cumul_NCGb_4",
        "Cumul_PMainboard",
        "Cumul_NCG_CO2",
        "Cumul_NG_CO2",
        "Cumul_El_CO2",
        "Cumul_avoid_CO2",
        "NCG",
        "Y3Fflow1",
        "Y3Fflow2",
        "Y3Fflow3",
        "Y3Fflow4",
        "PF_tot_1",
        "PF_tot_2",
        "PF_tot_3",
        "PF_tot_4",
        "PF_old",
        "NGb1",
        "NGb2",
        "NGb3",
        "NGb4",
        "NCGb1",
        "NCGb2",
        "NCGb3",
        "NCGb4",
        "PMainboard",
    ]
    Granularity = data["gran"]

    data_all["Vpump1_old"] = data_all["Vpump1_old"].fillna(0)
    data_all["Vpump2_old"] = data_all["Vpump2_old"].fillna(0)
    data_all["Vpump3_old"] = data_all["Vpump3_old"].fillna(0)
    data_all["Vpump4_old"] = data_all["Vpump4_old"].fillna(0)

    # Retrieve latest datapoint if first datapoint of a timeseries is NaN
    for column in data_all.columns:
        if np.isnan(data_all[column][0]):
            data_all[column][0] = (
                client.time_series.data.retrieve_latest(external_id=data[column]).to_pandas().iloc[0, 0]
            )

    data_all[data_all < 1] = 0  # Removing noise
    data_all = data_all.fillna(method="ffill")

    # Filter out wrong HO condensate values
    for lst in ["1", "2", "3", "4"]:
        data_all["CO" + lst][data_all["cond_pump" + lst] < data["threshold_cond_pump"]] = 0

    # merge Vacuum pump data
    data_all["Vpump1"] = data_all["Vpump1_old"] + data_all["Vpump1_new"]
    data_all["Vpump2"] = data_all["Vpump2_old"] + data_all["Vpump2_new"]
    data_all["Vpump3"] = data_all["Vpump3_old"] + data_all["Vpump3_new"]
    data_all["Vpump4"] = data_all["Vpump4_old"] + data_all["Vpump4_new"]

    # %% Balance per line
    deltaT = pd.Timedelta(Granularity)

    # Criteria for a line to be considered running (for the inclusion of old data in the cumulative sum)
    threshold_inscrew = data["threshold_inscrew"]  # Threshold for screw speed (%) to consider the line as working
    threshold_Vpump = data["threshold_Vpump"]  # %
    threshold_N2circ = data["threshold_N2circ"]  # %

    # Remove false NCG values
    data_all["NCG"][data_all["NCG"] < 20] = 0

    # Status verification
    for lst in ["1", "2", "3", "4"]:
        data_all["Status line " + lst] = "Standby"
        index1 = data_all[data_all["YFeedIn" + lst] > threshold_inscrew].index
        index2 = data_all[data_all["Vpump" + lst] > threshold_Vpump].index
        data_all["Status line " + lst][(data_all.index.isin(index1)) & (data_all.index.isin(index2))] = "Running"

        # Some 3 P sep flow data must be removed because it corresponds to the circulation heating mode of the vacuum pump
        data_all["Y3Fflow" + lst][
            (data_all["Vpump" + lst] <= threshold_Vpump) | (data_all["N2circ" + lst] >= threshold_N2circ)
        ] = 0

        # ----------------------------------------------------------------------
        # Remove 3P sep flow data where no data for HO or LO is available
        # since at least 15 min
        # ----------------------------------------------------------------------
        if deltaT < pd.Timedelta("15T"):
            if lst == "1" or lst == "2":
                mod = "1"
            elif lst == "3" or lst == "4":
                mod = "2"
            data_all["Last good " + lst] = pd.NaT
            data_all.loc[(data_all["HO" + lst] > 0), "Last good " + lst] = data_all[(data_all["HO" + lst] > 0)].index

            if data_all["Last good " + lst].isna().all():
                data_all["Last good " + lst] = pd.Timestamp(1998, 1, 1, 0, 0)
            elif data_all["Last good " + lst][0] is pd.NaT:
                data_all["Last good " + lst][0] = pd.Timestamp(1998, 1, 1, 0, 0)

            data_all["Last good " + lst] = data_all["Last good " + lst].fillna(method="ffill")
            data_all["Y3Fflow" + lst][
                ((data_all.index - data_all["Last good " + lst]) > pd.Timedelta("15T"))
                & (data_all["Status line " + lst] == "Standby")
            ] = 0

        # Remove false NG values
        data_all["NGb" + lst][data_all["NGb" + lst] > 120] = 0

        # Add HO from condensate tank to HO values, first setting evert CO value before Feb 25th 2022 to 0
        data_all["CO" + lst][
            (data_all.index - pd.Timestamp(year=2022, month=2, day=22, hour=0, minute=0)) < pd.Timedelta(0)
        ] = 0

    # Removing weird NG and NCG to burner data
    # For NCG, in the future might miss when a line is heated during heat up phase with NCG produced from another
    for lst in ["1", "2", "3", "4"]:
        data_all["NCGb" + lst][data_all["Status line " + lst] == "Standby"] = 0

    # Dataframe with cumulative values
    Cumulative = pd.DataFrame(
        0,
        index=data_all.index,
        columns=[
            "Cumul_feed_1",
            "Cumul_feed_2",
            "Cumul_feed_3",
            "Cumul_feed_4",
            "Cumul_feed_tot_prod",
            "Cumul_HO_1",
            "Cumul_HO_2",
            "Cumul_HO_3",
            "Cumul_HO_4",
            "Cumul_CO_1",
            "Cumul_CO_2",
            "Cumul_CO_3",
            "Cumul_CO_4",
            "Cumul_LO_M1",
            "Cumul_LO_M2",
            "Cumul_tot_Oil",
            "Cumul_W_M1",
            "Cumul_W_M2",
            "Cumul_NCG",
            "Cumul_runt_1",
            "Cumul_runt_2",
            "Cumul_runt_3",
            "Cumul_runt_4",
            "Cumul_NGb_1",
            "Cumul_NGb_2",
            "Cumul_NGb_3",
            "Cumul_NGb_4",
            "Cumul_NG_flare",
            "Cumul_NCGb_1",
            "Cumul_NCGb_2",
            "Cumul_NCGb_3",
            "Cumul_NCGb_4",
            "Cumul_PMainboard",
            "Cumul_NCG_CO2",
            "Cumul_NG_CO2",
            "Cumul_El_CO2",
            "Cumul_avoid_CO2",
        ],
    )

    df = data_all.loc[:, "Cumul_feed_1":"Cumul_avoid_CO2"]
    Cumulative.iloc[0, :] = df.iloc[0, :]

    Cumulative["Cumul_LO_M1"].iloc[1:] = data_all["LO_M1"].iloc[1:] * deltaT.seconds / 3600
    Cumulative["Cumul_LO_M2"].iloc[1:] = data_all["LO_M2"].iloc[1:] * deltaT.seconds / 3600
    Cumulative["Cumul_W_M1"].iloc[1:] = data_all["W_M1"].iloc[1:] * deltaT.seconds / 3600
    Cumulative["Cumul_W_M2"].iloc[1:] = data_all["W_M2"].iloc[1:] * deltaT.seconds / 3600
    Cumulative["Cumul_NCG"].iloc[1:] = data_all["NCG"].iloc[1:] * deltaT.seconds / 3600
    Cumulative["Cumul_PMainboard"].iloc[1:] = data_all["PMainboard"].iloc[1:] * deltaT.seconds / 3600  # kWh
    Cumulative["Cumul_NG_flare"].iloc[1:] = data_all["NGf"].iloc[1:] * deltaT.seconds / 3600

    # Check balance
    # check=pd.DataFrame(0, index=Cumulative.index, columns=['Line 1', 'Line 2', 'Line 3', 'Line 4'])

    for lst in ["1", "2", "3", "4"]:
        Cumulative["Cumul_HO_" + lst].iloc[1:] = data_all["HO" + lst].iloc[1:] * deltaT.seconds / 3600
        Cumulative["Cumul_CO_" + lst].iloc[1:] = data_all["CO" + lst].iloc[1:] * deltaT.seconds / 3600
        Cumulative["Cumul_feed_" + lst].iloc[1:] = data_all["PF_tot_" + lst].iloc[1:] * deltaT.seconds / 3600

        Cumulative["Cumul_runt_" + lst].iloc[1:][data_all["Status line " + lst].iloc[1:] == "Running"] = (
            1 * deltaT.seconds / 3600
        )

        Cumulative["Cumul_NGb_" + lst].iloc[1:] = data_all["NGb" + lst].iloc[1:] * deltaT.seconds / 3600
        Cumulative["Cumul_NCGb_" + lst].iloc[1:] = data_all["NCGb" + lst].iloc[1:] * deltaT.seconds / 3600

    # Total Cumulative feed based on products
    Cumulative["Cumul_feed_tot_prod"].iloc[1:] = data_all["PF_old"].iloc[1:] * deltaT.seconds / 3600

    # Total Cumulative Oil
    Cumulative["Cumul_tot_Oil"].iloc[1:] = (
        Cumulative["Cumul_HO_1"].iloc[1:]
        + Cumulative["Cumul_HO_2"].iloc[1:]
        + Cumulative["Cumul_HO_3"].iloc[1:]
        + Cumulative["Cumul_HO_4"].iloc[1:]
        + Cumulative["Cumul_CO_1"].iloc[1:]
        + Cumulative["Cumul_CO_2"].iloc[1:]
        + Cumulative["Cumul_CO_3"].iloc[1:]
        + Cumulative["Cumul_CO_4"].iloc[1:]
        + Cumulative["Cumul_LO_M1"].iloc[1:]
        + Cumulative["Cumul_LO_M2"].iloc[1:]
    )

    # Avoided CO2 emissions
    # From plastic not incinerated - Assume all plastic is -CH2-    Does not account for coke
    organic_feed = (
        Cumulative["Cumul_feed_1"].iloc[1:]
        + Cumulative["Cumul_feed_2"].iloc[1:]
        + Cumulative["Cumul_feed_3"].iloc[1:]
        + Cumulative["Cumul_feed_4"].iloc[1:]
        + Cumulative["Cumul_feed_4"].iloc[1:]
    ) * 0.9 - (Cumulative["Cumul_W_M1"].iloc[1:] + Cumulative["Cumul_W_M2"].iloc[1:])
    CO2_plastic = organic_feed.iloc[1:] * (12 / 14) * (44 / 12)  # kg
    # From Oil production avoided
    # Assumes 10 g CO2/MJ crude oil and LHV= 43 MJ/kg
    CO2_oil = (
        (
            Cumulative["Cumul_HO_1"].iloc[1:]
            + Cumulative["Cumul_HO_2"].iloc[1:]
            + Cumulative["Cumul_HO_3"].iloc[1:]
            + Cumulative["Cumul_HO_4"].iloc[1:]
            + Cumulative["Cumul_CO_1"].iloc[1:]
            + Cumulative["Cumul_CO_2"].iloc[1:]
            + Cumulative["Cumul_CO_3"].iloc[1:]
            + Cumulative["Cumul_CO_4"].iloc[1:]
            + Cumulative["Cumul_LO_M1"].iloc[1:]
            + Cumulative["Cumul_LO_M2"].iloc[1:]
        )
        * 10
        * 43
        / 1000
    )  # kg
    # Emissions
    # NCG - Assumes 0.75 kg C/kg NCG, based on historical data
    CO2_NCG = Cumulative["Cumul_NCG"].iloc[1:] * 0.75 * (44 / 12)
    # NG - Assumes only CH4
    CO2_NG = (
        (
            Cumulative["Cumul_NGb_1"].iloc[1:]
            + Cumulative["Cumul_NGb_2"].iloc[1:]
            + Cumulative["Cumul_NGb_3"].iloc[1:]
            + Cumulative["Cumul_NGb_4"].iloc[1:]
            + Cumulative["Cumul_NG_flare"].iloc[1:]
        )
        * (1e5 * 0.016 / (8.314 * 298.15))
        * (44 / 16)
    )
    # Electricity - Assumes Denmark mix 116 g/kWh: https://en.energinet.dk/About-our-news/News/2021/06/22/Danish-electricity-generation-was-greener-than-ever-in-2020
    CO2_el = Cumulative["Cumul_PMainboard"].iloc[1:] * data["elCO2"] / 1000

    Cumulative["Cumul_avoid_CO2"].iloc[1:] = CO2_plastic + CO2_oil - CO2_NCG - CO2_NG - CO2_el
    Cumulative["Cumul_NCG_CO2"].iloc[1:] = CO2_NCG
    Cumulative["Cumul_NG_CO2"].iloc[1:] = CO2_NG
    Cumulative["Cumul_El_CO2"].iloc[1:] = CO2_el

    Cumulative = Cumulative.cumsum(axis="index")
    Cumulative = Cumulative.fillna(method="ffill")

    # Check balance
    #     if lst=='1' or lst=='2':
    #         m='1'
    #     else:
    #         m='2'
    #     check['Line ' + lst]=(Cumulative['Cumul_feed_' + lst]-(Cumulative['Cumul_HO_' + lst] + Cumulative['Cumul_LO_M' + m] + Cumulative['Cumul_W_M' + m] + Cumulative['Cumul_NCG'])/0.9)
    # check['Total']=(Cumulative['Cumul_feed_1'] + Cumulative['Cumul_feed_2'] + Cumulative['Cumul_feed_3'] + Cumulative['Cumul_feed_4'])-(
    #     Cumulative['Cumul_HO_1'] + Cumulative['Cumul_HO_2'] + Cumulative['Cumul_HO_3'] + Cumulative['Cumul_HO_4'] + Cumulative['Cumul_LO_M1'] +
    #     Cumulative['Cumul_LO_M2'] + Cumulative['Cumul_W_M1'] + Cumulative['Cumul_W_M2'] + Cumulative['Cumul_NCG'])/0.9

    # %% Exporting time series to CDF - Taken from handle.py of the original feed calculation cognite function
    df_all = pd.DataFrame(index=Cumulative.index)  # .index[-1])

    for line in ["1", "2", "3", "4"]:

        # Export dataframe
        df_all["Cumulative_feed_" + line] = Cumulative["Cumul_feed_" + line]
        df_all["Cumulative_flash_tank_oil_" + line] = Cumulative["Cumul_HO_" + line]
        df_all["Cumulative_condensate_oil_" + line] = Cumulative["Cumul_CO_" + line]
        df_all["Cumulative_operating_hours_" + line] = Cumulative["Cumul_runt_" + line]
        df_all["Cumulative_NG_burner_" + line] = Cumulative["Cumul_NGb_" + line]
        df_all["Cumulative_NCG_burner_" + line] = Cumulative["Cumul_NCGb_" + line]

        # resp_pf = client.time_series.retrieve(
        #     external_id="Cumulative_feed_" + line,
        # )
        # resp_hop = client.time_series.retrieve(external_id="Cumulative_flash_tank_oil_" + line)
        # resp_cop = client.time_series.retrieve(external_id="Cumulative_condensate_oil_" + line)
        # resp_runt = client.time_series.retrieve(external_id="Cumulative_operating_hours_" + line)
        # resp_ngb = client.time_series.retrieve(external_id="Cumulative_NG_burner_" + line)
        # resp_ncgb = client.time_series.retrieve(external_id="Cumulative_NCG_burner_" + line)

        # create_timeseries_per_line(client, line, resp_pf, resp_hop, resp_cop, resp_runt, resp_ngb, resp_ncgb)

    for mod in ["1", "2"]:
        # Export dataframe
        df_all["Cumulative_light_oil_M" + mod] = Cumulative["Cumul_LO_M" + mod]
        df_all["Cumulative_water_M" + mod] = Cumulative["Cumul_W_M" + mod]

        # resp_lop = client.time_series.retrieve(external_id="Cumulative_light_oil_M" + mod)
        # resp_wp = client.time_series.retrieve(external_id="Cumulative_water_M" + mod)
        # create_timeseries_per_module(client, mod, resp_lop, resp_wp)

    df_all["Cumulative_ncg"] = Cumulative["Cumul_NCG"]
    df_all["Cumulative_NG_flare"] = Cumulative["Cumul_NG_flare"]
    df_all["Cumulative_avoided_CO2"] = Cumulative["Cumul_avoid_CO2"]
    df_all["Cumulative_NCG_CO2"] = Cumulative["Cumul_NCG_CO2"]
    df_all["Cumulative_NG_CO2"] = Cumulative["Cumul_NG_CO2"]
    df_all["Cumulative_El_CO2"] = Cumulative["Cumul_El_CO2"]
    df_all["Cumulative_El_consumption"] = Cumulative["Cumul_PMainboard"]  # kWh
    df_all["Cumulative_total_feed_products"] = Cumulative["Cumul_feed_tot_prod"]
    df_all["Cumulative_total_Oil"] = Cumulative["Cumul_tot_Oil"]

    # resp_ncg = client.time_series.retrieve(external_id="Cumulative_ncg")
    # resp_ngf = client.time_series.retrieve(external_id="Cumulative_NG_flare")
    # resp_avco2 = client.time_series.retrieve(external_id="Cumulative_avoided_CO2")
    # resp_ncgco2 = client.time_series.retrieve(external_id="Cumulative_NCG_CO2")
    # resp_ngco2 = client.time_series.retrieve(external_id="Cumulative_NG_CO2")
    # resp_elco2 = client.time_series.retrieve(external_id="Cumulative_El_CO2")
    # resp_el = client.time_series.retrieve(external_id="Cumulative_El_consumption")
    # resp_pftot = client.time_series.retrieve(external_id="Cumulative_total_feed_products")
    # resp_otot = client.time_series.retrieve(external_id="Cumulative_total_Oil")

    # create_timeseries_ncg(client, resp_ncg)
    # create_timeseries_ngf(client, resp_ngf)
    # create_timeseries_co2(client, resp_avco2, resp_ncgco2, resp_ngco2, resp_elco2)
    # create_timeseries_el(client, resp_el)
    # create_timeseries_pftot(client, resp_pftot)
    # create_timeseries_otot(client, resp_otot)

    dps_len = len(df_all)
    if dps_len > 0:
        client.time_series.data.insert_dataframe(df_all, external_id_headers=True, dropna=True)

    count = dps_len * 37

    print(f"{count} datapoints written")
    return count


# # %% Time serie creation function
# def create_timeseries_per_line(client, line, pf, hop, cop, rt, ngb, ncgb):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if pf is None:
#         ts1 = TimeSeries(
#             external_id="Cumulative_feed_" + line,
#             name="Cumulative_feed_" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative amount of plastic fed to line " + line,
#         )
#         resp = client.time_series.create(ts1)

#     if hop is None:
#         ts2 = TimeSeries(
#             external_id="Cumulative_flash_tank_oil_" + line,
#             name="Cumulative_flash_tank_oil_" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative amount of heavy oil collected from flash tank of line" + line,
#         )
#         resp = client.time_series.create(ts2)

#     if cop is None:
#         ts6 = TimeSeries(
#             external_id="Cumulative_condensate_oil_" + line,
#             name="Cumulative_condensate_oil_" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative amount of condensate oil collected from CMX01 line" + line,
#         )
#         resp = client.time_series.create(ts6)

#     if rt is None:
#         ts3 = TimeSeries(
#             external_id="Cumulative_operating_hours_" + line,
#             name="Cumulative_operating_hours_" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative operating hours for line " + line,
#         )
#         resp = client.time_series.create(ts3)

#     if ngb is None:
#         ts4 = TimeSeries(
#             external_id="Cumulative_NG_burner_" + line,
#             name="Cumulative_NG_burner_" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative natural gas consumption for line " + line,
#         )
#         resp = client.time_series.create(ts4)

#     if ncgb is None:
#         ts5 = TimeSeries(
#             external_id="Cumulative_NCG_burner_" + line,
#             name="Cumulative_NCG_burner_" + line,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative NCG consumption for line " + line,
#         )
#         resp = client.time_series.create(ts5)


# def create_timeseries_per_module(client, mod, lop, wp):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if lop is None:
#         ts1 = TimeSeries(
#             external_id="Cumulative_light_oil_M" + mod,
#             name="Cumulative_light_oil_M" + mod,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative amount of light oil from 3 phases separator of module " + mod,
#         )
#         resp = client.time_series.create(ts1)

#     if wp is None:
#         ts2 = TimeSeries(
#             external_id="Cumulative_water_M" + mod,
#             name="Cumulative_water_M" + mod,
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative amount of water separated from module " + mod,
#         )
#         resp = client.time_series.create(ts2)


# def create_timeseries_ncg(client, ncg):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if ncg is None:
#         ts = TimeSeries(
#             external_id="Cumulative_ncg",
#             name="Cumulative_ncg",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative amount of NCG produced",
#         )
#         resp = client.time_series.create(ts)


# def create_timeseries_ngf(client, ngf):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if ngf is None:
#         ts = TimeSeries(
#             external_id="Cumulative_NG_flare",
#             name="Cumulative_NG_flare",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative amount of NG to the flare",
#         )
#         resp = client.time_series.create(ts)


# def create_timeseries_co2(client, avco2, ncgco2, ngco2, elco2):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if avco2 is None:
#         ts = TimeSeries(
#             external_id="Cumulative_avoided_CO2",
#             name="Cumulative_avoided_CO2",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative avoided CO2",
#         )
#         resp = client.time_series.create(ts)

#     if ncgco2 is None:
#         ts1 = TimeSeries(
#             external_id="Cumulative_NCG_CO2",
#             name="Cumulative_NCG_CO2",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative CO2 from NCG combustion",
#         )
#         resp = client.time_series.create(ts1)

#     if ngco2 is None:
#         ts2 = TimeSeries(
#             external_id="Cumulative_NG_CO2",
#             name="Cumulative_NG_CO2",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative CO2 from NG combustion",
#         )
#         resp = client.time_series.create(ts2)

#     if elco2 is None:
#         ts3 = TimeSeries(
#             external_id="Cumulative_El_CO2",
#             name="Cumulative_El_CO2",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative CO2 from electricity consumption",
#         )
#         resp = client.time_series.create(ts3)


# def create_timeseries_el(client, el):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if el is None:
#         ts = TimeSeries(
#             external_id="Cumulative_El_consumption",
#             name="Cumulative_El_consumption",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative electricity consumption (MWh)",
#         )
#         resp = client.time_series.create(ts)


# def create_timeseries_pftot(client, pftot):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if pftot is None:
#         ts = TimeSeries(
#             external_id="Cumulative_total_feed_products",
#             name="Cumulative_total_feed_products",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative feed calculated based on products",
#         )
#         resp = client.time_series.create(ts)


# def create_timeseries_otot(client, otot):
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if otot is None:
#         ts = TimeSeries(
#             external_id="Cumulative_total_Oil",
#             name="Cumulative_total_Oil",
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Cumulative total oil collected in holding tank",
#         )
#         resp = client.time_series.create(ts)


# # handle(data, client)
