# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 17:28:40 2022

@author: EspenUlsetNordsveen
"""

import datetime

from requests import Session


dt = datetime.datetime


def handle(client):
    url = "https://api.energidataservice.dk/dataset/Elspotprices?offset=0&filter=%7B%22PriceArea%22:[%22DK1%22]%7D&sort=HourUTC%20DESC&timezone=dk"
    session = Session()
    r = session.get(url)

    for pos in range(len(r.json()["records"])):
        # time_local.append(datetime.datetime.strptime(r.json()['records'][pos]['HourDK'], '%Y-%m-%dT%H:%M:%S'))
        # dps.append(r.json()["records"][pos]["SpotPriceDKK"])
        time_local = datetime.datetime.strptime(r.json()["records"][pos]["HourDK"], "%Y-%m-%dT%H:%M:%S")
        dps = r.json()["records"][pos]["SpotPriceDKK"]
        client.datapoints.insert([(time_local, dps)], external_id="spotprice_el_Skive")
