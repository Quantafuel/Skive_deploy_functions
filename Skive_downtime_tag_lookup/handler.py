# -*- coding: utf-8 -*-
"""
Created on Tue May 13 12:39:38 2025

@author: Espen.Nordsveen
"""

# from cog_client import client


def handle(secrets, client):
    from common.utilities import Sharepoint

    CLIENT_ID = secrets.get("lists-id")
    CLIENT_SECRET = secrets.get("lists-secret")

    sharepoint = Sharepoint("S-Skive470", CLIENT_ID, CLIENT_SECRET, client)

    lists_id = sharepoint.get_lists_id()

    # Get tag data from sharepoint
    tag_lookup_data = sharepoint.get_list_data(lists_id, "TagLookup")

    # Add tags to raw
    sharepoint.tag_lookup_mapping(tag_lookup_data)
