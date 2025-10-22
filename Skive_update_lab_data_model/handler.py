# -*- coding: utf-8 -*-
"""
Created on Wed Oct 22 12:38:37 2025

@author: EspenNordsveen
"""


def handle(client):
    import re

    import pandas as pd

    from cognite.client.data_classes.data_modeling import DirectRelationReference, NodeApply, NodeOrEdgeData, ViewId

    my_view = ViewId("Skive_lab_data", "SimdistSamplesCombinedView", "91fb24e899f6ec")
    existing_nodes = []
    for chunk in client.data_modeling.instances(chunk_size=500, sources=my_view):
        df_chunk = chunk.to_pandas(expand_properties=True, camel_case=True)
        existing_nodes.append(df_chunk)

    existing_df = pd.concat(existing_nodes, ignore_index=True)
    print(f"Loaded {len(existing_df)} existing instances from Data Model")

    # Load simulation data
    raw_simdist = client.raw.rows.list("lab_db", "simdist_tb", limit=None)
    simdist_df = pd.DataFrame([r.columns for r in raw_simdist])
    print(f"Loaded {len(simdist_df)} rows from RAW.simdist_tb")

    # Load sample metadata
    raw_samples = client.raw.rows.list("lab_db", "samples_list_tb", limit=None)
    samples_df = pd.DataFrame([r.columns for r in raw_samples])
    print(f"Loaded {len(samples_df)} rows from RAW.samples_list_tb")

    # Build externalId
    simdist_df["externalId"] = "simdist_" + simdist_df["SampleID"].astype(str)

    # Cast numeric columns
    for col in [c for c in simdist_df.columns if c.startswith("C") or c.startswith("T")]:
        simdist_df[col] = pd.to_numeric(simdist_df[col], errors="coerce")

    # Prepare samples
    samples_df = samples_df.rename(
        columns={"ID": "sampleId", "SamplePoint": "samplePoint", "TestType": "testType", "Date": "sampleTime"}
    )
    samples_df["sampleId"] = pd.to_numeric(samples_df["sampleId"], errors="coerce")

    # Join simdist + samples
    simdist_df["SampleID"] = simdist_df["SampleID"].astype(str)
    samples_df["sampleId"] = samples_df["sampleId"].astype(str)
    combined_df = simdist_df.merge(samples_df, how="left", left_on="SampleID", right_on="sampleId")
    combined_df = combined_df.dropna(subset=["sampleId"])
    combined_df = combined_df.drop("SampleID", axis=1)
    print(f"Combined dataset: {len(combined_df)} rows")

    new_df = combined_df[~combined_df["externalId"].isin(existing_df["externalId"])].copy()
    new_df.columns = [c.lower() if re.match(r"^[CT]\d+$", c, re.IGNORECASE) else c for c in new_df.columns]
    new_df = new_df.rename(columns={"FBP": "fbp", "IBP": "ibp", "Flash_point": "flash_point"})
    # Ensure sampleTime is date-only in YYYY-MM-DD format
    new_df["sampleTime"] = pd.to_datetime(new_df["sampleTime"], errors="coerce").dt.strftime("%Y-%m-%d")
    new_df["recovered_at_250"] = new_df["recovered_at_250"].astype(float)
    new_df["recovered_at_350"] = new_df["recovered_at_350"].astype(float)
    new_df["flash_point"] = new_df["flash_point"].astype(float)
    new_df["fbp"] = new_df["fbp"].astype(float)
    new_df["ibp"] = new_df["ibp"].astype(float)
    new_df["sampleId"] = new_df["sampleId"].astype(int)

    print(f"Found {len(new_df)} new rows to insert into Data Model")

    for _, row in new_df.iterrows():
        node = NodeApply(
            space="Skive_lab_data",
            external_id=row["externalId"],
            sources=[
                NodeOrEdgeData(
                    ViewId("Skive_lab_data", "SimdistSamplesCombinedView", "91fb24e899f6ec"),
                    row.dropna()
                    .drop(
                        labels=[
                            "externalId",
                            "Recept",
                            "Sampler",
                            "SamplePointDescription",
                            "Status",
                            "key",
                            "Date",
                            "t5",
                        ],
                        errors="ignore",
                    )
                    .to_dict(),
                )
            ],
        )
        client.data_modeling.instances.apply(node)

    nodes_to_create = [
        NodeApply(
            space="Skive_lab_data",
            external_id=row["externalId"],
            type=DirectRelationReference("Skive_lab_data", "SimdistSamplesCombinedView"),
            sources={"SkiveLabData": row.dropna().to_dict()},
        )
        for _, row in new_df.iterrows()
    ]

    print(f"Uploaded {len(nodes_to_create)} new nodes")
