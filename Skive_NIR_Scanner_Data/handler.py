# data = {"ABS": "2s=TOMRA_MATERIAL_1",
#         "HDPE": "2s=TOMRA_MATERIAL_2",
#         "LDPE": "2s=TOMRA_MATERIAL_3",
#         "PET": "2s=TOMRA_MATERIAL_4",
#         "Plastic general": "2s=TOMRA_MATERIAL_5",
#         "PP": "2s=TOMRA_MATERIAL_6",
#         "PS": "2s=TOMRA_MATERIAL_7",
#         "PVC": "2s=TOMRA_MATERIAL_8",
#         "water": "2s=TOMRA_WATER_CONTENT_2:M_MID",
#         "peute": "schr_load_Peute_Folie",
#         "roaf": "schr_load_ROAF",
#         "total": "schr_load_Total_(Paprec)"}


# %% Total (8-19 february)
def handle(data, client):

    df_peute = client.time_series.data.retrieve_dataframe(
        external_id=[
            data["ABS"],
            data["HDPE"],
            data["LDPE"],
            data["PET"],
            data["Plastic general"],
            data["PP"],
            data["PS"],
            data["PVC"],
            data["water"],
        ],
        start="10w-ago",
        end="now",
        aggregates="average",
        granularity="1s",
        include_aggregate_name=False,
    )

    df_peute = df_peute.dropna()
    df_peute = df_peute.rename(
        columns={
            "2s=TOMRA_MATERIAL_1": "TOMRA_MATERIAL_ABS_percent",
            "2s=TOMRA_MATERIAL_2": "TOMRA_MATERIAL_HDPE_percent",
            "2s=TOMRA_MATERIAL_3": "TOMRA_MATERIAL_LDPE_percent",
            "2s=TOMRA_MATERIAL_4": "TOMRA_MATERIAL_PET_percent",
            "2s=TOMRA_MATERIAL_5": "TOMRA_MATERIAL_Plastic_general_percent",
            "2s=TOMRA_MATERIAL_6": "TOMRA_MATERIAL_PP_percent",
            "2s=TOMRA_MATERIAL_7": "TOMRA_MATERIAL_PS_percent",
            "2s=TOMRA_MATERIAL_8": "TOMRA_MATERIAL_PVC_percent",
            "2s=TOMRA_WATER_CONTENT_2:M_MID": "TOMRA_WATER_CONTENT_percent",
        }
    )

    selected_cols = df_peute.iloc[:, :-1]
    result = selected_cols.apply(lambda row: row / row.sum() * 100, axis=1)
    client.time_series.data.insert_dataframe(result)
