"""
Created on Tue Jan 18 11:23:27 2022

@author: SébastienPissot
"""


# %% Handle function
def handle(client):
    import numpy as np
    import pandas as pd

    data = {
        "agg": "average",
        "start_time": "121m-ago",
        "end_time": "now",
        "gran": "1m",
        "elCO2": 226,
        "C_NCG": 0.75,
        "density_NCG": 1.6,
        "CO2int_NG": 1.9,
        "NGb1": ["2s=L1_BURNER_NG_FLOW:Q_TT"],
        "NGb2": ["2s=L2_BURNER_NG_FLOW:Q_TT"],
        "NGb3": ["2s=L3_BURNER_NG_FLOW:Q_TT"],
        "NGb4": ["2s=L4_BURNER_NG_FLOW:Q_TT"],
        "NGf": ["2s=P10_QJD_BF601_DFLC01:Q_TT"],
        "NCGb1": ["2s=L1_BURNER_NCG_FLOW:Q_TT"],
        "NCGb2": ["2s=L2_BURNER_NCG_FLOW:Q_TT"],
        "NCGb3": ["2s=L3_BURNER_NCG_FLOW:Q_TT"],
        "NCGb4": ["2s=L4_BURNER_NCG_FLOW:Q_TT"],
        "NCGf": ["2s=P10_EKG_BF001_DFLC01:Q_TT"],
        "El": ["2s=P10_AAW01_BJ01:Q_TT"],
        "HO1": ["2s=P01_EGG_BF102_DFLC01:Q_TT"],
        "HO2": ["2s=P02_EGG_BF202_DFLC01:Q_TT"],
        "HO3": ["2s=P03_EGG_BF302_DFLC01:Q_TT"],
        "HO4": ["2s=P04_EGG_BF402_DFLC01:Q_TT"],
        "HHO1": ["2s=P01_EGG_BF103_DFLC01:Q_TT"],
        "HHO2": ["2s=P02_EGG_BF203_DFLC01:Q_TT"],
        "HHO3": ["2s=P03_EGG_BF303_DFLC01:Q_TT"],
        "HHO4": ["2s=P04_EGG_BF403_DFLC01:Q_TT"],
        "LO1": ["2s=P11_EGG_BF924_DFLC01:Q_TT"],
        "LO2": ["2s=P12_EGG_BF974_DFLC01:Q_TT"],
    }

    # Data with average aggregation
    all_ts_list_avg = data["NGb1"].copy()
    all_ts_list_avg.extend(data["NGb2"].copy())
    all_ts_list_avg.extend(data["NGb3"].copy())
    all_ts_list_avg.extend(data["NGb4"].copy())
    all_ts_list_avg.extend(data["NGf"].copy())
    all_ts_list_avg.extend(data["NCGb1"].copy())
    all_ts_list_avg.extend(data["NCGb2"].copy())
    all_ts_list_avg.extend(data["NCGb3"].copy())
    all_ts_list_avg.extend(data["NCGb4"].copy())
    all_ts_list_avg.extend(data["NCGf"].copy())
    all_ts_list_avg.extend(data["El"].copy())
    all_ts_list_avg.extend(data["HO1"].copy())
    all_ts_list_avg.extend(data["HO2"].copy())
    all_ts_list_avg.extend(data["HO3"].copy())
    all_ts_list_avg.extend(data["HO4"].copy())
    all_ts_list_avg.extend(data["HHO1"].copy())
    all_ts_list_avg.extend(data["HHO2"].copy())
    all_ts_list_avg.extend(data["HHO3"].copy())
    all_ts_list_avg.extend(data["HHO4"].copy())
    all_ts_list_avg.extend(data["LO1"].copy())
    all_ts_list_avg.extend(data["LO2"].copy())

    client.assets.list(limit=1)
    data_all = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list_avg,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=[data["agg"]],
        granularity=data["gran"],
    )

    data_all.columns = [
        "NGb1",
        "NGb2",
        "NGb3",
        "NGb4",
        "NGf",
        "NCGb1",
        "NCGb2",
        "NCGb3",
        "NCGb4",
        "NCGf",
        "El",
        "HO1",
        "HO2",
        "HO3",
        "HO4",
        "HHO1",
        "HHO2",
        "HHO3",
        "HHO4",
        "LO1",
        "LO2",
    ]

    # Retrieve latest datapoint if first datapoint of a timeseries is NaN
    for column in data_all.columns:
        if np.isnan(data_all[column][0]):
            data_all[column][0] = (
                client.time_series.data.retrieve_latest(external_id=data[column]).to_pandas().iloc[0, 0]
            )

    data_all = data_all.fillna(method="ffill")

    # %% Calculations
    NG_tot = data_all["NGb1"] + data_all["NGb2"] + data_all["NGb3"] + data_all["NGb4"] + data_all["NGf"]  # Nm3
    NCG_tot = (
        data_all["NCGb1"] * data["density_NCG"]
        + data_all["NCGb2"] * data["density_NCG"]
        + data_all["NCGb3"]
        + data_all["NCGb4"]
        + data_all["NCGf"]
    )  # kg
    Oil_tot = (
        data_all["HO1"]
        + data_all["HO2"]
        + data_all["HO3"]
        + data_all["HO4"]
        + data_all["HHO1"]
        + data_all["HHO2"]
        + data_all["HHO3"]
        + data_all["HHO4"]
        + data_all["LO1"]
        + data_all["LO2"]
    )  # kg

    CO2_NG = NG_tot * data["CO2int_NG"]  # data['CO2int_NG'] is kgCO2/Nm3 natural gas
    CO2_NCG = NCG_tot * data["C_NCG"] * (44 / 12)
    CO2_El = data_all["El"] * data["elCO2"] / 1000  # elCO2 in g/kWh and El in kWh
    CO2_tot = CO2_NG + CO2_NCG + CO2_El

    Output = pd.concat([NG_tot, NCG_tot, Oil_tot, CO2_NG, CO2_NCG, CO2_El, CO2_tot], axis="columns")
    Output = Output.rename(
        columns={0: "NG_tot", 1: "NCG_tot", 2: "Oil_tot", 3: "CO2_NG", 4: "CO2_NCG", "El": "CO2_El", 5: "CO2_tot"}
    )

    # %% Exporting time series to CDF - Taken from handle.py of the original feed calculation cognite function
    df_all = pd.DataFrame(index=Output.index)  # .index[-1])

    # Export dataframe
    df_all["Counter_total_NG"] = Output["NG_tot"]
    df_all["Counter_total_NCG"] = Output["NCG_tot"]
    df_all["Counter_total_Oil"] = Output["Oil_tot"]
    df_all["Counter_CO2_NG"] = Output["CO2_NG"]
    df_all["Counter_CO2_NCG"] = Output["CO2_NCG"]
    df_all["Counter_CO2_El"] = Output["CO2_El"]
    df_all["Counter_CO2_total"] = Output["CO2_tot"]

    dps_len = len(df_all)
    if dps_len > 0:
        client.time_series.data.insert_dataframe(df_all, external_id_headers=True, dropna=True)

    count = dps_len * 7

    print(f"{count} datapoints written")
    return count
