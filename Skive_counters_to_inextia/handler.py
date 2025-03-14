# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 11:59:00 2025

@author: Espen.Nordsveen
"""


def handle(client, secrets):
    from datetime import datetime

    import pandas as pd
    import requests

    USER_ID = secrets.get("inextia-user")
    USER_PWD = secrets.get("inextia-pwd")

    # %% Cognite data
    def get_latest_value(xid):
        # xid = "2s=" + tag_number.replace(".", "_")+":T_TT"
        latest_count = client.time_series.data.retrieve_latest(external_id=xid).value[0]
        return latest_count

    # df = pd.read_excel("C://Users//Espen.Nordsveen//OneDrive - Viridor\Dokumenter//Cognite//Cognite functions//Cognite-functions//Inextia//Cognite_tags_Tellere.xlsx")
    # df.drop(df.index[:1], inplace=True)

    # df_new = pd.DataFrame(df[["name", "Info", "Unit"]])

    # for row in df_new.iterrows():
    #     # print("2s="+row[1][0])
    #     print(row[1][0])
    #     try:
    #         counter = client.time_series.retrieve(external_id="2s="+row[1][0])
    #         latest_count = client.time_series.data.retrieve_latest(id=counter.id).value[0]
    #         print(latest_count)
    #     except Exception as e:
    #         print(f"Time series with external id {'2s=' + row[1][0]} does not exist.", e)

    # %% Inextia data

    def get_acces_token(base_url):
        endpoint = "/Auth"

        data = {"login": USER_ID, "password": USER_PWD}

        response = requests.post(base_url + endpoint, json=data)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to retrieve access token: {response.status_code}, {response.text}")

    def get_component(base_url, access_token, component_number):
        endpoint = f"/Components/{component_number}"
        # Make the request
        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        response = requests.get(url=base_url + endpoint, headers=headers)
        if response.status_code == 200:
            component = response.json()
            return component
        else:
            raise Exception(f"Failed to retrieve component: {response.status_code}, {response.text}")

    def get_counters(base_url, access_token):
        endpoint = "/Counters"
        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        response = requests.get(url=base_url + endpoint, headers=headers)
        if response.status_code == 200:
            counters = response.json()
            return counters
        else:
            raise Exception(f"Failed to retrieve counters: {response.status_code}, {response.text}")

    def get_counter_readings(base_url, access_token, tag_number):
        endpoint = "/Counters/" + f"{tag_number}" + "/readings"
        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        response = requests.get(url=base_url + endpoint, headers=headers)
        if response.status_code == 200:
            counters = response.json()
            return counters
        else:
            raise Exception(f"Failed to retrieve counters: {response.status_code}, {response.text}")

    def update_counter_reading(base_url, access_token, tag_number, last_counter_value):
        time = datetime.now()
        formatted_time = time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        endpoint = "/Counters/" + f"{tag_number}" + "/readings"

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        data = {"value": last_counter_value, "valueDate": formatted_time, "reset": False}
        print(base_url + endpoint)
        print(data)
        response = requests.post(base_url + endpoint, headers=headers, json=data)

        if response.status_code == 200:
            print("Counter value updated")
            if response.text:
                try:
                    return response.json()
                except ValueError:
                    print("Warning: Response is not valid JSON")
                    return response.text
            else:
                print("No content in response")
                return None
        else:
            raise Exception(f"Failed to update counter: {response.status_code}, {response.text}")

    base_url = "https://quantafuel.inextia.dk/api"
    access_token_dict = get_acces_token(base_url)
    access_token = access_token_dict.get("accessToken")

    try:
        df = pd.read_excel(
            "C://Users//Espen.Nordsveen//OneDrive - Viridor//Dokumenter//Cognite//Cognite functions//Cognite-functions//Inextia//xid_counter_import.xlsx"
        )
    except Exception as e:
        data = {
            "Counter": [
                "2s=P01_EGG_GQ101:T_TT",
                "2s=P02_EGG_GQ201:T_TT",
                "2s=P03_EGG_GQ301:T_TT",
                "2s=P04_EGG_GQ401:T_TT",
            ],
            "Description": [
                "pyrolysis linie 1 vacuumpumpe",
                "pyrolysis linie 2 vacuumpumpe",
                "pyrolysis linie 3 vacuumpumpe",
                "pyrolysis linie 4 vacuumpumpe",
            ],
        }
        df = pd.DataFrame(data)
        print("Dataframe created:", e)
    for row in df.iterrows():
        xid = row[1][0]
        tag_number = xid.replace("_", ".").split(":")[0][3:]
        last_counter_value = get_latest_value(xid)
        update_counter_reading(base_url, access_token, tag_number, last_counter_value)
    # get_component(base_url, access_token, "P01.EGG.GQ101")
    # counters = get_counters(base_url, access_token)
    # get_counter_readings(base_url, access_token, "P01.EGG.GQ101")
    #
