from datetime import datetime, timedelta


# This function will create new datapoints for the listed external IDs when the data is stale.
# This is done to be able to show continuous lines in grafana.
# To add tags to the function, add the external ID to the list
# The variable minutes_ago sets the max "age" of datapoints to be refreshed


def handle(client):

    ext_ids = [
        "2s=P01_INFD_PLAST_M_KG",
        "2s=P02_INFD_PLAST_M_KG",
        "2s=P03_INFD_PLAST_M_KG",
        "2s=P04_INFD_PLAST_M_KG",
        "2s=P01_QJD_QN606:M_POS",
        "2s=P02_QJD_QN606:M_POS",
        "2s=P03_QJD_QN606:M_POS",
        "2s=P04_QJD_QN606:M_POS",
        "2s=P02_QJD_BF603:M_MID",
        "2s=P02_EKG_QN613:M_POS",
        "2s=P02_EKG_BF604:M_MID",
        "2s=P03_QJD_BF603:M_MID",
        "2s=P03_EKG_QN613:M_POS",
        "2s=P03_EKG_BF604:M_MID",
        "2s=P04_EKG_BF604:M_MID",
    ]

    minutes_ago = 10

    now = datetime.now()
    td_ago = now - timedelta(minutes=minutes_ago)

    for i in ext_ids:
        latest_dp = client.time_series.data.retrieve_latest(external_id=i)
        latest_dp_datetime = datetime.fromtimestamp(latest_dp.timestamp[0] / 1000)

        if td_ago > latest_dp_datetime:
            dp_insert = [(now, latest_dp.value[0])]
            client.time_series.data.insert(dp_insert, external_id=i)
