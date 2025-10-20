def handle(data, client):
    """
    Applies filter of the order specified by the user. Filters supported
        1. Moving Average
    :param data:
    :param client:
    :return:
    """
    import numpy as np
    import pandas as pd

    print(data)
    print(data["input_timeseries"])

    dps = client.time_series.data.retrieve_dataframe(
        external_id=data["input_timeseries"],
        start=data["start_time"],
        end=data["end_time"],
        aggregates=[data["agg"]],
        granularity=data["gran"],
    )
    if data["filter_type"] == "moving_average":
        filtered_array = np.convolve(
            dps.values[:, 0], np.ones(data["filter_order"]) / data["filter_order"], mode="valid"
        )
        indices = dps.index[int(int(data["filter_order"]) / 2) : -int(int(data["filter_order"]) / 2)]
    else:
        return "Filter type not supported"
    filtered_df = pd.DataFrame(filtered_array, columns=[data["input_timeseries"] + "_filtered"], index=indices)
    filtered_df = filtered_df.dropna()
    dps_len = len(filtered_df)
    print(dps_len)
    resp = client.time_series.retrieve(external_id=data["input_timeseries"] + "_filtered")
    test_result = {
        "write_length": dps_len,
        "filtered_df_type": type(filtered_df.index),
        "filtered_df_name": list(filtered_df.columns),
    }

    # if resp is None:
    #     resp = create_timeseries(data, client)

    if dps_len > 0:
        client.time_series.data.insert_dataframe(filtered_df, external_id_headers=True)
        print(f"{dps_len} datapoints written")

    if "test" in data:
        return resp, test_result
    else:
        return dps_len


# Create time series
# def create_timeseries(data, client):
#     from cognite.client.data_classes import TimeSeries

#     orig_ts = client.time_series.retrieve(external_id=data["input_timeseries"])
#     ts = TimeSeries(
#         external_id=data["input_timeseries"] + "_filtered",
#         name=data["input_timeseries"] + "_filtered",
#         metadata={"Info": "Calculated from cognite functions"},
#         asset_id=orig_ts.asset_id,
#         description="Filtered data of " + orig_ts.name,
#     )
#     resp = client.time_series.create(ts)
#     return resp
