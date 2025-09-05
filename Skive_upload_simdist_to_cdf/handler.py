# -*- coding: utf-8 -*-
"""
Created on Thu May 15 23:36:52 2025

@author: Espen.Nordsveen
"""

# from cog_client import client
# import requests
# from dotenv import load_dotenv
# import os

# load_dotenv()
# CLIENT_ID = os.getenv("CLIENT_ID")
# CLIENT_SECRET = os.getenv("CLIENT_SECRET")


def handle(client, secrets):
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
            "grant_type": "client_credentials",  # Use "authorization_code" if using user-delegated permissions
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",  # Use ".default" for application permissions
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

        def transfer_simdist_files_from_sharepoint_to_cdf(self, drive_id):
            files_in_cdf = client.files.list(data_set_ids=5883851598140017, limit=None)
            file_names_from_cdf = [file.name for file in files_in_cdf]
            folder_path = "30 Operation/0010. LAB tests Skive/6 Analysis Results"

            list_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"

            headers = {"Authorization": f"Bearer {self.access_token}"}

            next_url = list_url
            while next_url:
                list_response = requests.get(next_url, headers=headers)
                if list_response.status_code != 200:
                    print("Error listing files:", list_response.status_code, list_response.text)
                    break

                data = list_response.json()
                items = data.get("value", [])
                for item in items:
                    if "file" in item:
                        file_name = item["name"].split(".")[0] + "_simdist_report." + item["name"].split(".")[1]
                        download_url = item["@microsoft.graph.downloadUrl"]
                        file_response = requests.get(download_url)
                        if file_response.status_code == 200 and file_name not in file_names_from_cdf:
                            file_bytes = file_response.content
                            print(file_name)
                            client.files.upload_bytes(
                                file_bytes,
                                name=file_name,
                                source="SharePoint",
                                mime_type="text/csv",  # or actual MIME type if known
                                data_set_id=5883851598140017,
                            )

                        else:
                            if file_response.status_code != 200:
                                print(f"Failed to download file {file_name}: {file_response.status_code}")
                            else:
                                print(f"File {file_name} already uplaoded to CDF!")

                next_url = data.get("@odata.nextLink")

    sharepoint = MSListData("S-Skive470")
    site_id = sharepoint.get_site_id("S-Skive470")
    # files = client.files.list(data_set_ids=8915881263906932, limit=None)
    drive_id = sharepoint.get_drive_id(site_id)
    sharepoint.transfer_simdist_files_from_sharepoint_to_cdf(drive_id)
