"""
Created on Mon Mar 14 19:30:44 2022

@author: ThomasBergIversen
"""


# %% Handle function and list generation
def handle(data, client):

    import numpy as np
    import pandas as pd

    def LMTDcalc(hsin, hsout, csin, csout):
        dTA = abs(hsin - csout)
        dTB = abs(hsout - csin)
        LMTD = (dTA - dTB) / np.log(dTA / dTB)
        return LMTD

    data = {
        "agg": "average",
        "start_time": "2h-ago",
        "end_time": "now",
        "gran": "1m",
        "BT104": ["2s=P01_EGG_BT104_MTMP01:M_MID"],
        "BT204": ["2s=P02_EGG_BT204_MTMP01:M_MID"],
        "BT304": ["2s=P03_EGG_BT304_MTMP01:M_MID"],
        "BT404": ["2s=P04_EGG_BT404_MTMP01:M_MID"],
        "BT105": ["2s=P01_EGG_BT105_106_MTMP01:M_MID"],
        "BT205": ["2s=P02_EGG_BT205_206_MTMP01:M_MID"],
        "BT305": ["2s=P03_EGG_BT305_306_MTMP01:M_MID"],
        "BT405": ["2s=P04_EGG_BT405_406_MTMP01:M_MID"],
        "BF120": ["2s=P01_PAB_BF120_MFLW01:M_MID"],
        "BF220": ["2s=P02_PAB_BF220_MFLW01:M_MID"],
        "BF320": ["2s=P03_PAB_BF320_MFLW01:M_MID"],
        "BF420": ["2s=P04_PAB_BF420_MFLW01:M_MID"],
        "BT121": ["2s=P01_PAB_BT121_MTMP01:M_MID"],
        "BT221": ["2s=P02_PAB_BT221_MTMP01:M_MID"],
        "BT321": ["2s=P03_PAB_BT321_MTMP01:M_MID"],
        "BT421": ["2s=P04_PAB_BT421_MTMP01:M_MID"],
        "BT122": ["2s=P01_PAB_BT122_MTMP01:M_MID"],
        "BT222": ["2s=P02_PAB_BT222_MTMP01:M_MID"],
        "BT322": ["2s=P03_PAB_BT322_MTMP01:M_MID"],
        "BT422": ["2s=P04_PAB_BT422_MTMP01:M_MID"],
        "BDP804": ["2s=P10_PAB_BDP804_MPRS01:M_MID"],
        "POSQN112": ["2s=P01_PAB_QN112:M_POS"],
        "POSQN212": ["2s=P02_PAB_QN212:M_POS"],
        "POSQN312": ["2s=P03_PAB_QN312:M_POS"],
        "POSQN412": ["2s=P04_PAB_QN412:M_POS"],
        "BT807_808": ["2s=P10_PAB_BT807_808_MTMP01:M_MID"],
        "BT132": ["2s=P01_PAB_BT132_MTMP01:M_MID"],
        "BT232": ["2s=P02_PAB_BT232_MTMP01:M_MID"],
        "BT332": ["2s=P03_PAB_BT332_MTMP01:M_MID"],
        "BT432": ["2s=P04_PAB_BT432_MTMP01:M_MID"],
        "BT107": ["2s=P01_EGG_BT107_MTMP01:M_MID"],
        "BT207": ["2s=P02_EGG_BT207_MTMP01:M_MID"],
        "BT307": ["2s=P03_EGG_BT307_MTMP01:M_MID"],
        "BT407": ["2s=P04_EGG_BT407_MTMP01:M_MID"],
        "BT108": ["2s=P01_EGG_BT108_109_MTMP01:M_MID"],
        "BT208": ["2s=P02_EGG_BT208_209_MTMP01:M_MID"],
        "BT308": ["2s=P03_EGG_BT308_309_MTMP01:M_MID"],
        "BT408": ["2s=P04_EGG_BT408_409_MTMP01:M_MID"],
    }

    # %%

    counter = 0
    for key, val in data.items():
        if isinstance(val, list):
            if counter == 0:
                all_ts_list = data[key].copy()
                HeaderList = [str(key)]
                counter += 1
            else:
                all_ts_list.extend(data[key].copy())
                HeaderList.append(str(key))

    # %% Extracing data
    data_all = client.time_series.data.retrieve_dataframe(
        external_id=all_ts_list,
        start=data["start_time"],
        end=data["end_time"],
        aggregates=[data["agg"]],
        granularity=data["gran"],
    )

    data_all.columns = HeaderList
    # Granularity = data["gran"]

    # %% Filtring data
    for column in data_all.columns:
        if np.isnan(data_all[column][0]):
            data_all[column][0] = (
                client.time_series.data.retrieve_latest(external_id=data[column]).to_pandas().iloc[0, 0]
            )

    data_all[data_all < 1] = 0  # Removing noise
    data_all = data_all.fillna(method="ffill")

    # %% EPx02 duty, LMTD and UA
    # See also functional description for reference to these calculations
    data_all["P_EP102"] = 2.15 * data_all["BF120"] * 900 / 3600 * (data_all["BT122"] - data_all["BT121"])
    data_all["P_EP202"] = 2.15 * data_all["BF220"] * 900 / 3600 * (data_all["BT222"] - data_all["BT221"])
    data_all["P_EP302"] = 2.15 * data_all["BF320"] * 900 / 3600 * (data_all["BT322"] - data_all["BT321"])
    data_all["P_EP402"] = 2.15 * data_all["BF420"] * 900 / 3600 * (data_all["BT422"] - data_all["BT421"])

    data_all["LMTD_EP102"] = LMTDcalc(data_all["BT104"], data_all["BT105"], data_all["BT121"], data_all["BT122"])
    data_all["LMTD_EP202"] = LMTDcalc(data_all["BT204"], data_all["BT205"], data_all["BT221"], data_all["BT222"])
    data_all["LMTD_EP302"] = LMTDcalc(data_all["BT304"], data_all["BT305"], data_all["BT321"], data_all["BT322"])
    data_all["LMTD_EP402"] = LMTDcalc(data_all["BT404"], data_all["BT405"], data_all["BT421"], data_all["BT422"])

    data_all["UA_EP102"] = data_all["P_EP102"] / data_all["LMTD_EP102"]
    data_all["UA_EP202"] = data_all["P_EP202"] / data_all["LMTD_EP202"]
    data_all["UA_EP302"] = data_all["P_EP302"] / data_all["LMTD_EP302"]
    data_all["UA_EP402"] = data_all["P_EP402"] / data_all["LMTD_EP402"]

    # %% EPx03 duty, LMTD and UA
    # BFx12; estimated flow (based on equal percentage valve with KVs = 25)
    for line in [1, 2, 3, 4]:
        data_all["BF" + str(line) + "12"] = (
            np.exp((3.912 * (data_all["POSQN" + str(line) + "12"]) / 100)) * 0.5 * np.sqrt(data_all["BDP804"] / 0.96)
        )
        data_all["BF" + str(line) + "12"][data_all["POSQN" + str(line) + "12"] == 0] = 0

    data_all["P_EP103"] = 2.15 * (data_all["BF112"]) * 900 / 3600 * (data_all["BT132"] - data_all["BT807_808"])
    data_all["P_EP203"] = 2.15 * (data_all["BF212"]) * 900 / 3600 * (data_all["BT232"] - data_all["BT807_808"])
    data_all["P_EP303"] = 2.15 * (data_all["BF312"]) * 900 / 3600 * (data_all["BT332"] - data_all["BT807_808"])
    data_all["P_EP403"] = 2.15 * (data_all["BF412"]) * 900 / 3600 * (data_all["BT432"] - data_all["BT807_808"])

    data_all["LMTD_EP103"] = LMTDcalc(data_all["BT107"], data_all["BT108"], data_all["BT807_808"], data_all["BT132"])
    data_all["LMTD_EP203"] = LMTDcalc(data_all["BT207"], data_all["BT208"], data_all["BT807_808"], data_all["BT232"])
    data_all["LMTD_EP303"] = LMTDcalc(data_all["BT307"], data_all["BT308"], data_all["BT807_808"], data_all["BT332"])
    data_all["LMTD_EP403"] = LMTDcalc(data_all["BT407"], data_all["BT408"], data_all["BT807_808"], data_all["BT432"])

    data_all["UA_EP103"] = data_all["P_EP103"] / data_all["LMTD_EP103"]
    data_all["UA_EP203"] = data_all["P_EP203"] / data_all["LMTD_EP203"]
    data_all["UA_EP303"] = data_all["P_EP303"] / data_all["LMTD_EP303"]
    data_all["UA_EP403"] = data_all["P_EP403"] / data_all["LMTD_EP403"]

    # %% Exporting time series to CDF - Taken from handle.py of the original feed calculation cognite function
    df_all = pd.DataFrame(index=data_all.index)
    count = 0

    for line in [1, 2, 3, 4]:
        #         # Export dataframe
        df_all["P_EP" + str(line) + "02"] = data_all["P_EP" + str(line) + "02"]
        df_all["LMTD_EP" + str(line) + "02"] = data_all["LMTD_EP" + str(line) + "02"]
        df_all["UA_EP" + str(line) + "02"] = data_all["UA_EP" + str(line) + "02"]
        df_all["P_EP" + str(line) + "03"] = data_all["P_EP" + str(line) + "03"]
        df_all["LMTD_EP" + str(line) + "03"] = data_all["LMTD_EP" + str(line) + "03"]
        df_all["UA_EP" + str(line) + "03"] = data_all["UA_EP" + str(line) + "03"]
        dps_len = len(df_all)

        count += dps_len * 6
        if dps_len > 0:
            client.time_series.data.insert_dataframe(df_all, external_id_headers=True, dropna=True)
    print(f"{count} datapoints written")
    return count
