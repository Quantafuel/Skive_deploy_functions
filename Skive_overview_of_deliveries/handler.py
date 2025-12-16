# -*- coding: utf-8 -*-
"""
Created on Fri Dec 12 16:10:11 2025

@author: EspenNordsveen
"""


def handle(client, secrets):
    from io import BytesIO

    import pandas as pd
    import requests

    CLIENT_ID = secrets.get("lists-id")
    CLIENT_SECRET = secrets.get("lists-secret")

    class MSListData:
        """
        A class to represent Sharepoint list data in Skive. The class fetches data from Sharepoint lists in the Viridor domain

        ...

        Attributes
        ----------
        site_name : str
            the name of the sharepoint site to fetch data from

        Methods
        -------
        get_access_token(cls):
            Class method. Returns the access_token

        get_site_id(cls, site_name)
            Class method. Returns the site id for the given site_name


        """

        access_token = None

        # OAuth 2.0 token endpoint
        token_url = "https://login.microsoftonline.com/92bce3bb-abfb-484b-b074-32e1a37f3631/oauth2/v2.0/token"

        # Request payload
        payload = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
        }

        # Class method to obtain the access token
        @classmethod
        def get_access_token(cls):
            """
            Obtains the access token from sharepoint with the given CLIENT_ID and CLIENT_SECRET for the app registration in Azure,
            set up for sharepoint access to Viridor domain. CLIENT_ID and CLIENT_SECRET is stored in a .env file. Obtains a token
            valid globally inside the class instance

            Returns
            -------
            str
                DESCRIPTION.

            """
            if cls.access_token is None:
                # Make the POST request
                response = requests.post(cls.token_url, data=cls.payload)

                # Handle the response
                if response.status_code == 200:
                    cls.access_token = response.json().get("access_token")

                    # print("Access Token:", access_token)
                else:
                    print(f"Failed to obtain token: {response.status_code}, {response.text}")
            return cls.access_token

        @classmethod
        def get_site_id(cls, site_name):

            access_token = cls.get_access_token()

            hostname = "viridor.sharepoint.com"

            # API endpoint
            url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_name}"

            # Make the request
            headers = {
                "Authorization": f"Bearer {access_token}",
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                site_id = response.json().get("id")
                # print("Site ID:", site_id)
                if not site_id:
                    raise Exception("Site ID not found in the response.")
                return site_id
            else:
                raise Exception(f"Failed to retrieve site ID: {response.status_code}, {response.text}")

        def __init__(self, site_name):
            """
            Construct all the necessary attributes for the MSListData object

            Parameters
            ----------
            site_name : str
                name of the sharepoint site to fetch data from

            Returns
            -------
            None.

            """
            self.access_token = MSListData.get_access_token()
            self.site_id = MSListData.get_site_id(site_name)
            self.site_name = site_name
            print(f"Initialized MSListData with Site name {site_name} and Site ID: {self.site_id}")

        def get_drive_id(self, site_id):
            drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive"
            headers = {"Authorization": f"Bearer {self.access_token}"}

            drive_response = requests.get(drive_url, headers=headers)
            drive_id = drive_response.json()["id"]
            return drive_id

        def find_path(self, drive_id):
            headers = {"Authorization": f"Bearer {self.access_token}"}
            root_list_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
            response = requests.get(root_list_url, headers=headers)
            for item in response.json().get("value", []):
                print(item["name"], "📁" if "folder" in item else "📄")
                print(item["webUrl"])
            return response

        def find_file_from_path(self, drive_id, folder_path, file_name):
            """


            Parameters
            ----------
            drive_id : str
                DESCRIPTION.
            folder_path : str
                DESCRIPTION.
            file_name : str
                DESCRIPTION.

            Returns
            -------
            file_content : BytesIO
                DESCRIPTION.

            """

            folder_path = "20 Supply and Procurement/12. Supply and Laboratory"

            list_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"

            headers = {"Authorization": f"Bearer {self.access_token}"}

            list_response = requests.get(list_url, headers=headers)
            response = list_response.json()
            items = response.get("value")
            for item in items:
                if file_name == item.get("name"):
                    download_url = item["@microsoft.graph.downloadUrl"]
                    file_response = requests.get(download_url)
                    file_content = BytesIO(file_response.content)
                    return file_content
            return None

    sharepoint = MSListData("S-Skive470")
    folder_path = "20 Supply and Procurement/12. Supply and Laboratory"
    file_name = "Overview over deliveries 2025.xlsx"

    site_id = sharepoint.get_site_id("S-Skive470")
    # files = client.files.list(data_set_ids=8915881263906932, limit=None)
    drive_id = sharepoint.get_drive_id(site_id)

    file_content = sharepoint.find_file_from_path(drive_id, folder_path, file_name)

    df_deliveries = pd.read_excel(file_content)

    df_deliveries = df_deliveries.dropna(axis=0, subset=["Filling date"])
    df_deliveries["Filling date"] = df_deliveries["Filling date"].dt.strftime("%Y-%m-%d")
    df_deliveries["Dispatch date"] = df_deliveries["Dispatch date"].dt.strftime("%Y-%m-%d")
