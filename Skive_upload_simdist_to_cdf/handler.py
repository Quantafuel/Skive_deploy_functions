# -*- coding: utf-8 -*-


def handle(client, secrets):
    import requests

    CLIENT_ID = secrets.get("lists-id")
    CLIENT_SECRET = secrets.get("lists-secret")

    DATA_SET_ID = 5883851598140017
    MAX_FILES_PER_RUN = 50
    REQUEST_TIMEOUT = 15

    class MSListData:
        access_token = None

        token_url = "https://login.microsoftonline.com/92bce3bb-abfb-484b-b074-32e1a37f3631/oauth2/v2.0/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
        }

        @classmethod
        def get_access_token(cls):
            if cls.access_token is None:
                response = requests.post(cls.token_url, data=cls.payload, timeout=REQUEST_TIMEOUT)

                if response.status_code == 200:
                    cls.access_token = response.json().get("access_token")
                else:
                    raise Exception(f"Token error: {response.status_code}, {response.text}")

            return cls.access_token

        @classmethod
        def get_site_id(cls, site_name):
            access_token = cls.get_access_token()
            hostname = "viridor.sharepoint.com"

            url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{site_name}"
            headers = {"Authorization": f"Bearer {access_token}"}

            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                site_id = response.json().get("id")
                if not site_id:
                    raise Exception("Site ID missing")
                return site_id
            else:
                raise Exception(f"Site error: {response.status_code}, {response.text}")

        def __init__(self, site_name):
            self.access_token = self.get_access_token()
            self.site_id = self.get_site_id(site_name)
            self.site_name = site_name

        def get_drive_id(self):
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive"
            headers = {"Authorization": f"Bearer {self.access_token}"}

            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if response.status_code != 200:
                raise Exception(f"Drive error: {response.status_code}, {response.text}")

            return response.json()["id"]

        def transfer_files(self, client):
            headers = {"Authorization": f"Bearer {self.access_token}"}

            existing_files = {f.name for f in client.files.list(data_set_ids=DATA_SET_ID, limit=None)}

            folder_path = "30 Operation/0010. LAB tests Skive/6 Analysis Results"
            next_url = f"https://graph.microsoft.com/v1.0/drives/{self.get_drive_id()}/root:/{folder_path}:/children"

            processed = 0

            while next_url and processed < MAX_FILES_PER_RUN:
                response = requests.get(next_url, headers=headers, timeout=REQUEST_TIMEOUT)

                if response.status_code != 200:
                    raise Exception(f"List error: {response.status_code}, {response.text}")

                data = response.json()

                for item in data.get("value", []):
                    if processed >= MAX_FILES_PER_RUN:
                        break

                    if "file" not in item:
                        continue

                    name_parts = item["name"].rsplit(".", 1)
                    if len(name_parts) != 2:
                        continue

                    file_name = f"{name_parts[0]}_simdist_report.{name_parts[1]}"

                    if file_name in existing_files:
                        continue

                    download_url = item["@microsoft.graph.downloadUrl"]

                    file_response = requests.get(download_url, timeout=REQUEST_TIMEOUT + 10)

                    if file_response.status_code != 200:
                        print(f"Download failed: {file_name}")
                        continue

                    client.files.upload_bytes(
                        file_response.content,
                        name=file_name,
                        source="SharePoint",
                        mime_type="text/csv",
                        data_set_id=DATA_SET_ID,
                    )

                    print(f"Uploaded: {file_name}")
                    existing_files.add(file_name)
                    processed += 1

                next_url = data.get("@odata.nextLink")

            print(f"Processed {processed} new files")

    sharepoint = MSListData("S-Skive470")
    sharepoint.transfer_files(client)
