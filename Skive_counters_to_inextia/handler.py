# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 11:59:00 2025

@author: Espen.Nordsveen
"""


def handle(client, secrets):
    import re

    from datetime import datetime

    import requests

    USER_ID = secrets.get("inextia-user")
    USER_PWD = secrets.get("inextia-pwd")

    data = {
        "P01.EAC.GL001-M101": "2s=P01EAC01GL001M101:T_TT",
        "P01.EAC.GL002-M102": "2s=P01EAC01GL002M102:T_TT",
        "P01.EAC.GL002-M102-2": "2s=P01EAC01GL002M1022:T_TT",
        "P01.EAC.GL003-M103": "2s=P01EAC01GL003M103:T_TT",
        "P01.EAC.GL004-M104": "2s=P01EAC01GL004M104:T_TT",
        "P01.EAC.GL005-M105": "2s=P01EAC01GL005M105:T_TT",
        "P01.EGG.GP101": "2s=P01_EGG_GP101:T_TT",
        "P01.EGG.GP102": "2s=P01_EGG_GP102:T_TT",
        "P01.EGG.GP103-MA01": "2s=P01_EGG_GP103:T_TT",
        "P01.EGG.GP106": "2s=P01_EGG_GP106:T_TT",
        "P01.EGG.GP107": "2s=P01_EGG_GP107:T_TT",
        "P01.EGG.GQ101": "2s=P01_EGG_GQ101:T_TT",
        "P01.ECC.HX001-R101": "P01ECC01HX001R101:T_TT",
        "P01.ETG.GL001": "2s=P01ETG01GL001:T_TT",
        "P01.ETG.GL002-S101A": "2s=P01ETG01GL002S101A:T_TT",
        "P01.ETG.GL003-S102": "2s=P01ETG01GL003S102:T_TT",
        "P01.ETG.GL004-S102A": "2s=P01ETG01GL004S102A:T_TT",
        "P01.RAA.GQ001-C101": "2s=P01RAA01GQ001C101:T_TT",
        "P02.EAC.GL001-M201": "2s=P02EAC02GL001M201:T_TT",
        "P02.EAC.GL002-M202": "2s=P02EAC02GL002M202:T_TT",
        "P02.EAC.GL002-M202-2": "2s=P02EAC02GL002M2022:T_TT",
        "P02.EAC.GL003-M203": "2s=P02EAC02GL003M203:T_TT",
        "P02.EAC.GL004-M204": "2s=P02EAC02GL004M204:T_TT",
        "P02.EAC.GL005-M205": "2s=P02EAC02GL005M205:T_TT",
        "P02.ECC.HX001-R201": "2s=P02ECC02HX001R201:T_TT",
        "P02.EGG.GP201": "2s=P02_EGG_GP201:T_TT",
        "P02.EGG.GP202": "2s=P02_EGG_GP202:T_TT",
        "P02.EGG.GP203-MA01": "2s=P02_EGG_GP203:T_TT",
        "P02.EGG.GP206-MA01": "2s=P02_EGG_GP206:T_TT",
        "P02.EGG.GP207": "2s=P02_EGG_GP207:T_TT",
        "P02.EGG.GQ201": "2s=P02_EGG_GQ201:T_TT",
        "P02.ETG.GL001": "2s=P02ETG02GL001:T_TT",
        "P02.ETG.GL002-S201A": "2s=P02ETG02GL002S201A:T_TT",
        "P02.ETG.GL003-S202": "2s=P02ETG02GL003S202:T_TT",
        "P02.ETG.GL004-S202A": "2s=P02ETG02GL004S202A:T_TT",
        "P02.PAB.GP221": "2s=P02_PAB_GP221:T_TT",
        "P02.RAA.GQ001-C201": "2s=P02RAA02GQ001C201:T_TT",
        "P03.EAC.GL001-M301": "2s=P03EAC03GL001M301:T_TT",
        "P03.EAC.GL002-M302": "2s=P03EAC03GL002M302:T_TT",
        "P03.EAC.GL002-M302-2": "2s=P03EAC03GL002M3022:T_TT",
        "P03.EAC.GL003-M303": "2s=P03EAC03GL003M303:T_TT",
        "P03.EAC.GL004-M304": "2s=P03EAC03GL004M304:T_TT",
        "P03.EAC.GL005-M305": "2s=P03EAC03GL005M305:T_TT",
        "P03.ECC.HX001-R301": "2s=P03ECC03HX001R301:T_TT",
        "P03.EGG.GP301": "2s=P03_EGG_GP301:T_TT",
        "P03.EGG.GP302": "2s=P03_EGG_GP302:T_TT",
        "P03.EGG.GP303": "2s=P03_EGG_GP303:T_TT",
        "P03.EGG.GP306": "2s=P03_EGG_GP306:T_TT",
        "P03.EGG.GP307": "2s=P03_EGG_GP307:T_TT",
        "P03.EGG.GQ301": "2s=P03_EGG_GQ301:T_TT",
        "P03.ETG.GL001": "2s=P03ETG03GL001:T_TT",
        "P03.ETG.GL002": "2s=P03ETG03GL002:T_TT",
        "P03.ETG.GL002-S301A": "2s=P03ETG03GL002S301A:T_TT",
        "P03.ETG.GL003-S302": "2s=P03ETG03GL003S302:T_TT",
        "P03.ETG.GL004": "2s=P03ETG03GL004:T_TT",
        "P03.ETG.GL004-S302A": "2s=P03ETG03GL004S302A:T_TT",
        "P03.PAB.GP321": "2s=P03_PAB_GP321:T_TT",
        "P03.RAA.GQ001-C301": "2s=P03RAA03GQ001C301:T_TT",
        "P04.EAC.GL001-M401": "2s=P04EAC04GL001M401:T_TT",
        "P04.EAC.GL002-M402": "2s=P04EAC04GL002M402:T_TT",
        "P04.EAC.GL002-M402-2": "2s=P04EAC04GL002M4022:T_TT",
        "P04.EAC.GL003-M403": "2s=P04EAC04GL003M403:T_TT",
        "P04.EAC.GL004-M404": "2s=P04EAC04GL004M404:T_TT",
        "P04.EAC.GL005-M405": "2s=P04EAC04GL005M405:T_TT",
        "P04.ECC.HX001-R401": "2s=P04ECC04HX001R401:T_TT",
        "P04.EGG.GP401": "2s=P04_EGG_GP401:T_TT",
        "P04.EGG.GP402": "2s=P04_EGG_GP402:T_TT",
        "P04.EGG.GP403-MA01": "2s=P04_EGG_GP403:T_TT",
        "P04.EGG.GP406": "2s=P04_EGG_GP406:T_TT",
        "P04.EGG.GP406-MA01": "2s=P04_EGG_GP406-MA01:T_TT",
        "P04.EGG.GP407": "2s=P04_EGG_GP407:T_TT",
        "P04.EGG.GQ401": "2s=P04_EGG_GQ401:T_TT",
        "P04.ETG.GL001": "2s=P04ETG04GL001:T_TT",
        "P04.ETG.GL002": "2s=P04ETG04GL002:T_TT",
        "P04.ETG.GL002-S401A": "2s=P04ETG04GL002S401A:T_TT",
        "P04.ETG.GL003-S402": "2s=P04ETG04GL003S402:T_TT",
        "P04.ETG.GL004": "2s=P04ETG04GL004:T_TT",
        "P04.ETG.GL004-S402A": "2s=P04ETG04GL004S402A:T_TT",
        "P04.HLA.GQ601": "2s=P04_HLA_GQ601:T_TT",
        "P04.PAB.GP421": "2s=P04_PAB_GP421:T_TT",
        "P04.RAA.GQ001-C401": "2s=P04RAA04GQ001C401:T_TT",
        "P10.EAC.GL002-MA001": "2s=P10EAC01GL002MA001:T_TT",
        "P10.EAC.GL003.MA001": "2s=P10EAC01GL003MA001:T_TT",
        "P10.EAC.GL003-MA001": "2s=P10EAC01GL003MA001:T_TT",
        "P10.EAC.GL004-MA001": "2s=P10EAC01GL004MA001:T_TT",
        "P10.EAC.GL005-MA001": "2s=P10EAC01GL005MA001:T_TT",
        "P10.EAC.GL006-HU02": "2s=P10EAC01GL006HU02:T_TT",
        "P10.EAC.GL007-MA001": "2s=P10EAC01GL007MA001:T_TT",
        "P10.EAC.GL008-MA001": "2s=P10EAC01GL008MA001:T_TT",
        "P10.EAC.GL009-HU02": "2s=P10EAC01GL009HU02:T_TT",
        "P10.EAC.GL010-MA001": "2s=P10EAC01GL010MA001:T_TT",
        "P10.EAC.GL011-MA001": "2s=P10EAC01GL011MA001:T_TT",
        "P10.EGG.MA501": "2s=P10_EGG_MA501:T_TT",
        "P10.EGG.MA511": "2s=P10_EGG_MA511:T_TT",
        "P10.EGG.MA521": "2s=P10_EGG_MA521:T_TT",
        "P10.EKG.GQ01A": "2s=P10_EKG_GQ01A:T_TT",
        "P10.EKG.GQ01B": "2s=P10_EKG_GQ01B:T_TT",
        "P10.ETA.GL003-MA003": "2s=P10_ETA_GL003:T_TT",
        "P10.ETA.GL003-QQ02": "2s=P10_ETA_GL003-QQ02:T_TT",
        "P10.ETA.GL004-MA004": "2s=P10_ETA_GL004:T_TT",
        "P10.ETA.GL005-MA011": "2s=P10_ETA_GL005:T_TT",
        "P10.ETA.GL006-MA021": "2s=P10_ETA_GL006:T_TT",
        "P10.ETG.GL001-MA001": "2s=P10_ETG_GL001:T_TT",
        "P10.ETG.GL002-MA002": "2s=P10_ETG_GL002:T_TT",
        "P10.GNK.GP003": "2s=P10_GNK_GP003:T_TT",
        "P10.GQ013": "2s=P10_XAA_GQ013:T_TT",
        "P10.GUA.GP001": "2s=P10_GUA_GP001:T_TT",
        "P10.GUA.GP002": "2s=P10_GUA_GP002:T_TT",
        "P10.GUA.GP004": "2s=P10_GUA_GP004:T_TT",
        "P10.NEB.GP001": "2s=P10_NEB_GP001:T_TT",
        "P10.NEB.GP002": "2s=P10_NEB_GP002:T_TT",
        "P10.PAB.GP801": "2s=P10_PAB_GP801:T_TT",
        "P10.PAB.GP802": "2s=P10_PAB_GP802:T_TT",
        "P10.PAB.GP803": "2s=P10_PAB_GP803:T_TT",
        "P10.PAB.GP804": "2s=P10_PAB_GP804:T_TT",
        "P10.PAB.GP806": "2s=P10_PAB_GP806:T_TT",
        "P10.PAB.GP807": "2s=P10_PAB_GP807:T_TT",
        "P10.PAB.GP851": "2s=P10_PAB_GP851:T_TT",
        "P10.PAB.GP852": "2s=P10_PAB_GP852:T_TT",
        "P10.PAB.GP853": "2s=P10_PAB_GP853:T_TT",
        "P10.PAB.GP854": "2s=P10_PAB_GP854:T_TT",
        "P10.PAB.GP911": "2s=P11_PAB_GP911:T_TT",
        "P10.PAB.GP912": "2s=P11_PAB_GP912:T_TT",
        "P10.PAB.GP961": "2s=P12_PAB_GP961:T_TT",
        "P10.PAB.GP962": "2s=P12_PAB_GP962:T_TT",
        "P10.PCB.GP021": "2s=P10_PCB_GP021:T_TT",
        "P10.PCB.GP022": "2s=P10_PCB_GP022:T_TT",
        "P10.PCB.GP131": "2s=P01_PCB_GP131:T_TT",
        "P10.PCB.GP231": "2s=P02_PCB_GP231:T_TT",
        "P10.PCB.GP331": "2s=P03_PCB_GP331:T_TT",
        "P10.PCB.GP431": "2s=P04_PCB_GP431:T_TT",
        "P10.QEA.GQ001": "2s=P10_QEA_GQ001:T_TT",
        "P10.QEA.GQ002": "2s=P10_QEA_GQ002:T_TT",
        "P10.SUZ.GP01A": "2s=P10_SUZ_GP01A:T_TT",
        "P10.SUZ.GP01B": "2s=P10_SUZ_GP01B:T_TT",
        "P10.SUZ.GP02A": "2s=P10_SUZ_GP02A:T_TT",
        "P10.SUZ.GP02B": "2s=P10_SUZ_GP02B:T_TT",
        "P10.SUZ.GP03A": "2s=P10_SUZ_GP03A:T_TT",
        "P10.SUZ.GP03B": "2s=P10_SUZ_GP03B:T_TT",
        "P10.XAA.GQ001A-MA01": "2s=P10_XAA_GQ001A:T_TT",
        "P10.XAA.GQ001B-MA01": "2s=P10_XAA_GQ001B:T_TT",
        "P10.XAA.GQ002A.MA01": "2s=P10_XAA_GQ002A:T_TT",
        "P10.XAA.GQ002B.MA01": "2s=P10_XAA_GQ002B:T_TT",
        "P10.XAA.GQ003A-MA01": "2s=P10_XAA_GQ003A:T_TT",
        "P10.XAA.GQ003B-MA01": "2s=P10_XAA_GQ003B:T_TT",
        "P10.XAA.GQ004A-MA01": "2s=P10_XAA_GQ004A:T_TT",
        "P10.XAA.GQ004B.MA01": "2s=P10_XAA_GQ004B:T_TT",
        "P10.XAA.GQ005-MA01": "2s=P10_XAA_GQ005:T_TT",
        "P10.XAA.GQ006-MA01": "2s=P10_XAA_GQ006:T_TT",
        "P10.XAA.Walkingfloor": "2s=P10_XAA_Walkingfloor:T_TT",
        "P10_EGG73_BF511": "2s=P10_EGG73_BF511:T_TT",
        "P10-værksted-udsugning": "2s=P10-værksted-udsugning:T_TT",
        "P11.EGG.GP921": "2s=P11_EGG_GP921:T_TT",
        "P11.EGG.GP922": "2s=P11_EGG_GP922:T_TT",
        "P11.GNK.GP922": "2s=P11_GNK_GP922:T_TT",
        "P11.RAA.GQ901-MA01": "2s=P11_RAA_GQ901:T_TT",
        "P12.EGG.GP971": "2s=P12_EGG_GP971:T_TT",
        "P12.EGG.GP972": "2s=P12_EGG_GP972:T_TT",
        "P12.GNK.GP972": "2s=P12_GNK_GP972:T_TT",
    }

    # %% Cognite data
    def get_latest_value(xid):
        # xid = "2s=" + tag_number.replace(".", "_")+":T_TT"
        latest_count = client.time_series.data.retrieve_latest(external_id=xid).value[0]
        return int(round(latest_count, 0))

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

        params = {"page": 1, "pageSize": 200}
        response = requests.get(url=base_url + endpoint, headers=headers, params=params)
        if response.status_code == 200:
            counters = response.json()
            return counters
        else:
            raise Exception(f"Failed to retrieve counters: {response.status_code}, {response.text}")

    def get_counter_numbers(counters):
        counter_numbers = []
        for counter in counters:
            counter_numbers.append(counter.get("counterNo"))
        return counter_numbers

    def verify_counter_xid(counter_list, counter_dict):
        missing_xid = []
        for counter in counter_list:
            if counter in counter_dict:
                counter_xid = counter_dict[counter]
                print(counter, " - ", counter_xid)
            else:
                print(f"Counter {counter} not found in dict mapping")
                continue
            ts = client.time_series.retrieve(external_id=counter_xid)
            if ts is not None:
                print(f"Time series {ts.external_id} found")
            else:
                print(f"Time series with xid {counter_xid} not found")
                missing_xid.append(counter_xid)
        return missing_xid

    def clean_counter_xid(dirty_list):
        clean_list = []
        for counter in dirty_list:
            if "XAA" in counter:
                new_xid = re.sub(r"[-_]MA01", "", counter)  # Matches either "-MA01" or "_MA01"
                clean_list.append(new_xid)
            if "EAC" and "M" in counter:
                new_xid = re.sub(r"[-_](?=[^:]*:T_TT)", "", counter)
                clean_list.append(new_xid)
        return clean_list

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

    counters = get_counters(base_url, access_token)
    counter_list = get_counter_numbers(counters)

    for counter in counter_list:
        if counter in data:
            counter_xid = data[counter]
        else:
            print(f"Counter {counter} not found in dict mapping")
            continue
        ts = client.time_series.retrieve(external_id=counter_xid)
        if ts is not None:
            print(f"Time series {ts.external_id} found")
            last_counter_value = get_latest_value(counter_xid)
            try:
                update_counter_reading(base_url, access_token, counter, last_counter_value)
            except Exception as e:
                print("Error updating counter:", e)
                continue
        else:
            print(f"Time series with xid {counter_xid} not found")
