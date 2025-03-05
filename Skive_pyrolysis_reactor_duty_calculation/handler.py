# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 19:30:44 2022

@author: ThomasBergIversen
"""


def handle(client):
    import numpy as np
    import pandas as pd

    data = {
        "agg": "average",
        "start_time": "2h-ago",
        "end_time": "now",
        "gran": "1s",
        "YT_B1": ["2s=P01ECC01TR111:M_MID"],
        "YT_B2": ["2s=P02ECC02TR211:M_MID"],
        "YT_B3": ["2s=P03ECC03TR311:M_MID"],
        "YT_B4": ["2s=P04ECC04TR411:M_MID"],
        "YT_FGIN1": ["2s=P01_RAA_TRC101_102_MTMP01:M_MID"],
        "YT_FGIN2": ["2s=P02_RAA_TRC201_202_MTMP01:M_MID"],
        "YT_FGIN3": ["2s=P03_RAA_TRC301_302_MTMP01:M_MID"],
        "YT_FGIN4": ["2s=P04_RAA_TRC401_402_MTMP01:M_MID"],
        "YT_FGOUT1": ["2s=P01_RAA_BT120_MTMP01:M_MID"],
        "YT_FGOUT2": ["2s=P02_RAA_BT220_MTMP01:M_MID"],
        "YT_FGOUT3": ["2s=P03_RAA_BT320_MTMP01:M_MID"],
        "YT_FGOUT4": ["2s=P04_RAA_BT420_MTMP01:M_MID"],
        "NGb1": ["2s=P01_QJD_BF603:M_MID"],
        "NGb2": ["2s=P02_QJD_BF603:M_MID"],
        "NGb3": ["2s=P03_QJD_BF603:M_MID"],
        "NGb4": ["2s=P04_QJD_BF603:M_MID"],
        "NCGb1": ["NCG_burner_1"],
        "NCGb2": ["NCG_burner_2"],
        "NCGb3": ["NCG_burner_3"],
        "NCGb4": ["NCG_burner_4"],
        "YF_Air1": ["2s=P01_HLA_BF601:M_MID"],
        "YF_Air2": ["2s=P02_HLA_BF601:M_MID"],
        "YF_Air3": ["2s=P03_HLA_BF601:M_MID"],
        "YF_Air4": ["2s=P04_HLA_BF601:M_MID"],
        "YHRdP1": ["2s=P01_RAA_BDP101:M_MID"],
        "YHRdP2": ["2s=P02_RAA_BDP201:M_MID"],
        "YHRdP3": ["2s=P03_RAA_BDP301:M_MID"],
        "YHRdP4": ["2s=P04_RAA_BDP401:M_MID"],
        "YHRSpeed1": ["2s=P01RAA01GQ001C101:M_HAST"],
        "YHRSpeed2": ["2s=P02RAA02GQ001C201:M_HAST"],
        "YHRSpeed3": ["2s=P03RAA03GQ001C301:M_HAST"],
        "YHRSpeed4": ["2s=P04RAA04GQ001C401:M_HAST"],
        "PF1": ["New_Plastic_feed_1_filtered"],
        "PF2": ["New_Plastic_feed_2_filtered"],
        "PF3": ["New_Plastic_feed_3_filtered"],
        "PF4": ["New_Plastic_feed_4_filtered"],
        "W1": ["New_Water_separated_1_filtered"],
        "W2": ["New_Water_separated_2_filtered"],
        "W3": ["New_Water_separated_3_filtered"],
        "W4": ["New_Water_separated_4_filtered"],
    }

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
    # Granularity = data['gran']

    # %% Filtring data
    for column in data_all.columns:
        if np.isnan(data_all[column][0]):
            data_all[column][0] = (
                client.time_series.data.retrieve_latest(external_id=data[column], before=data["start_time"])
                .to_pandas()
                .iloc[0, 0]
            )

    data_all[data_all < 1] = 0  # Removing noise
    data_all = data_all.fillna(method="ffill")

    # %% Pyrolysis reactor duty balance
    content = data_all

    # Fuel assumptions:
    # NG
    NGb_LHV = 36  # MJ/Nm3 From: 12080101-03469 02D - Driftsveiledning for Røykgass -og Naturgassystemet - Validated with Evida
    d_NG = 0.8
    # NCG
    NCG_LHV = 70  # MJ/m3, from average of PEUTE - EUROFINS report gave 68-73 MJ/m3 DRY GAS!
    d_NCG = 1.6
    w_NCG = 0.04  # vol% assumed based on simulation results

    content_local = pd.DataFrame(index=content.index)
    counter = 0
    linelist = [1, 2, 3, 4]
    for line in linelist:
        #     content_local['COil'+str(line)][COilGPx01['GP'+str(line)+'01'] < 1 ] = 0
        #     HHO['HHO'+str(line)]=COilGPx01['COil'+str(line)]

        # TP_IN   = content['YT_F']
        # TP_IN   = 20

        # FILTERING 1 out of 2

        content[content["NCGb" + str(line)] > 100] = 0  # filtering out high NCG values when not operating

        # END FILTERING 1 out of 2

        TP_OUT = content["YT_B" + str(line)]  # check
        TF_IN = content["YT_FGIN" + str(line)]  # check
        TF_OUT = content["YT_FGOUT" + str(line)]  # check
        F_NGb = content["NGb" + str(line)]  # Nm3/h
        m_NCGb = content["NCGb" + str(line)]  # kg/h
        if line == 4:
            m_NCGb = (
                m_NCGb / 1.6
            )  # Reason for this is that L4 already has units of kg/h in SCADA with assumption of 1.6 kg/m3
        F_Air = content["YF_Air" + str(line)]  # Nm3/h
        # H   = content['YHRdP'+str(line)]
        # N   = content['YHRSpeed'+str(line)]

        # Duty calculation
        # Duty
        TPLASTIC = 20.0
        TP_IN = pd.Series([TPLASTIC for x in range(len(content.index))], index=content.index)
        dTA = TF_IN - TP_IN
        dTB = TF_OUT - TP_OUT

        d_Air = 1.29  # kg/Nm3 http://www.uigi.com/air_quantity_convert.html
        # cp_air  = 1.006     #cpa = 1.006 (kJ/kg*C) https://www.engineeringtoolbox.com/air-specific-heat-capacity-d_705.html (dry air)
        # cp_air  = 1.5     #Niklas' table
        d_CO2 = 1.842  # https://www.engineeringtoolbox.com/gas-density-d_158.html
        d_O2 = 1.331  # https://www.engineeringtoolbox.com/gas-density-d_158.html
        d_N2 = 1.165  # https://www.engineeringtoolbox.com/gas-density-d_158.html
        d_H2O = 0.804  # https://www.engineeringtoolbox.com/gas-density-d_158.html

        M_CO2 = 44.01  # g/mol
        M_O2 = 16.00  # g/mol
        M_N2 = 28.01  # g/mol
        M_H2O = 18.02  # g/mol

        # vol fractions
        X_CO2 = 0.036
        X_O2 = 0.113
        X_H2O = 0.112
        X_N2 = 1 - (X_CO2 + X_O2 + X_H2O)

        d_denomin = X_CO2 * d_CO2 + X_O2 * d_O2 + X_H2O * d_H2O + X_N2 * d_N2
        # d_FG    = d_denomin

        x_CO2 = d_CO2 * X_CO2 / d_denomin
        x_O2 = d_O2 * X_O2 / d_denomin
        x_H2O = d_H2O * X_H2O / d_denomin
        x_N2 = d_N2 * X_N2 / d_denomin
        # sumcheck= x_CO2 + x_O2 + x_H2O + x_N2

        Cp_CO2 = (
            52  # J/molK https://webbook.nist.gov/cgi/cbook.cgi?ID=C124389&Units=SI&Mask=1&Type=JANAFG&Plot=on#JANAFG
        )
        Cp_O2 = 34  # J/molK https://webbook.nist.gov/cgi/cbook.cgi?ID=C7782447&Type=JANAFG&Plot=on
        Cp_H2O = (
            40  # J/molK https://webbook.nist.gov/cgi/cbook.cgi?ID=C7732185&Units=SI&Mask=1F&Type=JANAFG&Plot=on#JANAFG
        )
        Cp_N2 = 32  # J/molK https://webbook.nist.gov/cgi/cbook.cgi?ID=C7727379&Mask=1&Type=JANAFG&Plot=on#JANAFG

        Cp_MIX = (
            (Cp_CO2 * x_CO2 / M_CO2) + (Cp_O2 * x_O2 / M_O2) + (Cp_N2 * x_N2 / M_N2) + (Cp_H2O * x_H2O / M_H2O)
        )  # J/gK or kJ/kgK or kJ/kgC for dT

        # content_local['Q'+str(line)]     = NGb_LHV*F_NGb - (((F_Air*d_FG + F_NGb*d_Air) * Cp_MIX * (TF_OUT-TPLASTIC))/1000)  # MJ/h
        content_local["Q" + str(line)] = (
            NGb_LHV * F_NGb
            + NCG_LHV * m_NCGb / d_NCG * (1 - w_NCG)
            - (((F_Air * d_Air + F_NGb * d_NG + m_NCGb) * Cp_MIX * (TF_OUT - TPLASTIC)) / 1000)
        )  # MJ/h
        content_local["Q" + str(line)] = content_local["Q" + str(line)] / 3.6  # kW

        # DutyLoss =   ((((F_Air*d_FG - F_NGb*2*d_Air) * Cp_MIX * (TF_OUT-TPLASTIC))/1000)) / (3.6 * content_local['Q'+str(line)])

        # Heat loss
        content_local["HLoss" + str(line)] = 300  # kW
        content_local["Qbal" + str(line)] = content_local["Q" + str(line)] - content_local["HLoss" + str(line)]  # kW

        HPlastic = 1316.1  # kJ/kg https://nottingham-repository.worktribe.com/OutputFile/924962

        content_local["HPlastic" + str(line)] = (
            content["PF" + str(line)] * HPlastic / 3600
        )  # kg/h*kJ/kg = kJ/h *(1h/3600s) = kJ/s = kW

        # UA and LMTD
        # LMTD - Assuming parallel flow
        content_local["LMTD" + str(line)] = (dTA - dTB) / np.log(dTA / dTB)

        content_local["UA" + str(line)] = content_local["Q" + str(line)] / content_local["LMTD" + str(line)]  # kW/(gC)

        # AKilnEst = 113     # Not including external finns or internals. Estimate via CHM's CFD model
        # content_local['U']    = (content_local['UA']/AKilnEst)*1000    # W/(gC m2)

        # % Specific duties
        content_local["SpecDuty" + str(line)] = content_local["Q" + str(line)] / content["PF" + str(line)]
        content_local["SpecDutyHL" + str(line)] = (content_local["Qbal" + str(line)]) / content["PF" + str(line)]

        # % Fan speed related duty
        # # Acquired from A. Meeg
        # k_1  = 0.000789041787653074
        # k_2  = 5.93591964413509E-11
        # k_3  = 3

        # # content_local['Q local'] = ((𝑘_1 * (N*17.8)^2−H*100)/𝑘_2 )**(1/𝑘_3)
        # content_local['YHRFlowVol']   = ( (k_1*(N*17.8)**2 - H*100)/k_2 )**(1/k_3)
        # content_local['YHRFlowMass']  = ((content_local['YHRFlowVol'] / (content['YT_FGIN']+content['YT_FGOUT'])/2)*273)*d_FG

        # content_local['QRecirc']      = (content_local['YHRFlowMass'] * (content['YT_FGIN']-content['YT_FGOUT']) * Cp_MIX)/3600

        # %% Duty: Water
        # Source = https://www.engineeringtoolbox.com/saturated-steam-properties-d_101.html
        # From 20 to 100 degC
        h1_20deg = 251  # kJ/kg
        h1_100deg = 419  # kJ/kg
        h1 = h1_100deg - h1_20deg
        # Evaporation at 1 bara
        h2 = 2257  # kJ/kg
        # Superheating to 440 degC
        h3_100deg = 2676  # kJ/kg
        h3_440deg = 2742  # kJ/kg
        h3 = h3_440deg - h3_100deg

        h_water = h1 + h2 + h3  # kJ/kg
        # content_local['YWatercontent'] = content['W']/content['PF']
        content_local["QWater" + str(line)] = (
            h_water * content["W" + str(line)] / 3600
        )  # kJ/kg * kg/h = kJ/h *1h/3600s=kW

        # %%
        # FILTERING 2 out of 2

        content_local["SpecDuty" + str(line)][content["PF" + str(line)] < 50] = np.nan
        content_local["SpecDuty" + str(line)][content_local["SpecDuty" + str(line)] < 0] = np.nan
        content_local["SpecDuty" + str(line)][content_local["SpecDuty" + str(line)] > 5] = np.nan

        # END FILTERING 2 out of 2

        # %%

        # Just for understanding data
        content_local["NGb" + str(line)] = content["NGb" + str(line)]  # Nm3/h
        content_local["NCGb" + str(line)] = content["NCGb" + str(line)]  # kg/h
        content_local["PF" + str(line)] = content["PF" + str(line)]  # kg/h

        counter += 1
        if counter == 1:
            indexnames = [0] * (len(linelist) * len(content_local.columns.tolist()))
            for line in linelist:
                counter2 = line
                for i in content_local.columns.tolist():
                    indexnames[counter2 - 1] = i[:-1] + str(line)
                    counter2 += len(linelist)

    # Sort content_local
    content_local = content_local.reindex(indexnames, axis=1)

    # Shifting timezone - OBS! Raw parameters
    # content_local = content_local.tz_localize('CET', ambiguous=True)

    # %% Exporting time series to CDF - Taken from handle.py of the original feed calculation cognite function
    df_all = pd.DataFrame()
    count = 0

    # content_local['Q'+str(line)], content_local['SpecDuty'+str(line)], content_local['QWater'+str(line)]

    for line in [1, 2, 3, 4]:
        #         # Export dataframe
        df_all["Q" + str(line)] = content_local["Q" + str(line)]
        df_all["SpecDuty" + str(line)] = content_local["SpecDuty" + str(line)]
        df_all["QWater" + str(line)] = content_local["QWater" + str(line)]
        dps_len = len(df_all)

        # resp_Q = client.time_series.retrieve(external_id="Q" + str(line))
        # resp_SpecDuty = client.time_series.retrieve(external_id="SpecDuty" + str(line))
        # resp_QWater = client.time_series.retrieve(external_id="QWater" + str(line))
        # create_timeseries(client, line, resp_Q, resp_SpecDuty, resp_QWater)

        count += dps_len * 3
        if dps_len > 0:
            client.time_series.data.insert_dataframe(df_all, external_id_headers=True, dropna=True)
        print(f"{count} datapoints written")

    return count


# #%% Time serie creation function
# def create_timeseries(client, line, Q, SpecDuty, QWater):
#     print('f2')
#     asset = client.assets.retrieve(external_id="Quantafuel_Skive")
#     if Q is None:
#         ts1 = TimeSeries(
#             external_id="Q" + str(line),
#             name="Q" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Calculated duty based on (1) fired to pyrolysis reactor minus the (2) heat in flue gas leaving the system. [kW]",
#         )
#         resp = client.time_series.create(ts1)
#     if SpecDuty is None:
#         ts2 = TimeSeries(
#             external_id="SpecDuty" + str(line),
#             name="SpecDuty" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Calculated duty (Q) divided by filtered plastic feed [kWh/kg plastic]",
#         )
#         resp = client.time_series.create(ts2)

#     if QWater is None:
#         ts3 = TimeSeries(
#             external_id="QWater" + str(line),
#             name="QWater" + str(line),
#             metadata={"Info": "Calculated from cognite functions"},
#             asset_id=asset.id,
#             description="Estimated amount of heat required to heat up the amount of water coming with the plastic. [kW]",
#         )
#         resp = client.time_series.create(ts3)


# handle(data, client)
