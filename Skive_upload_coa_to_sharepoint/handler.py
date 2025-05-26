# -*- coding: utf-8 -*-
"""
Created on Tue May  6 16:04:45 2025

@author: Espen.Nordsveen
"""


def handle(client, secrets):
    import requests

    CLIENT_ID = secrets.get("lists-id")
    CLIENT_SECRET = secrets.get("lists-secret")

    class Sharepoint:
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
            Construct all the necessary attributes for the Sharepoint object

            Parameters
            ----------
            site_name : str
                name of the sharepoint site to fetch data from

            Returns
            -------
            None.

            """
            self.access_token = Sharepoint.get_access_token()
            self.site_id = Sharepoint.get_site_id(site_name)
            self.site_name = site_name
            print(f"Initialized Sharepoint with Site name {site_name} and Site ID: {self.site_id}")

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

        def get_files_from_folder(self, drive_id):
            folder_path = "30 Operation/0010. LAB tests Skive/7 Certificate Of Analysis"
            list_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"

            headers = {"Authorization": f"Bearer {self.access_token}"}
            list_response = requests.get(list_url, headers=headers)
            if list_response.status_code == 200:
                print("Success")
                items = list_response.json()["value"]
                for item in items:
                    print(f"{item['name']}")
            else:
                print("Error listing files:", list_response.status_code)
                print(list_response.text)
            return items

        def upload_file_to_sharepoint(self, drive_id, file_name, file_bytes):
            folder_path = "30 Operation/0010. LAB tests Skive/7 Certificate Of Analysis"
            upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}/{file_name}:/content"

            headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/octet-stream"}

            upload_response = requests.put(upload_url, headers=headers, data=file_bytes)
            if upload_response.status_code in [200, 201]:
                print(f"Uploaded: {file_name}")
            else:
                print(f"Failed to upload {file_name}: {upload_response.status_code}")
                print(upload_response.text)

    sharepoint = Sharepoint("S-Skive470")
    site_id = sharepoint.get_site_id("S-Skive470")
    files = client.files.list(data_set_ids=8915881263906932, limit=None)
    drive_id = sharepoint.get_drive_id(site_id)
    sharepoint_files = sharepoint.get_files_from_folder(drive_id)
    files_in_sharepoint = []
    for file in sharepoint_files:
        files_in_sharepoint.append(file.get("name"))

    try:
        for file in files:
            file_name = file.name
            if file_name not in files_in_sharepoint:
                file_content = client.files.download_bytes(id=file.id)
                sharepoint.upload_file_to_sharepoint(drive_id, file_name, file_content)
                print(file_name, "uploaded to sharepoint")
    except Exception as e:
        print("No files to upload:", e)
