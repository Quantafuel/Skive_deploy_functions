import requests


class Sharepoint:
    """
    A class to represent Sharepoint list data. Fetches data from Sharepoint lists in the specified domain.

    Attributes
    ----------
    site_name : str
        The name of the Sharepoint site to fetch data from.
    client_id : str
        Azure AD application client ID.
    client_secret : str
        Azure AD application client secret.

    Methods
    -------
    get_access_token():
        Returns the access_token.

    get_site_id(site_name):
        Returns the site ID for the given site name.
    """

    token_url = "https://login.microsoftonline.com/92bce3bb-abfb-484b-b074-32e1a37f3631/oauth2/v2.0/token"
    graph_hostname = "viridor.sharepoint.com"

    def __init__(self, site_name, client_id, client_secret, client):
        self.client_id = client_id
        self.client_secret = client_secret
        self.site_name = site_name
        self.access_token = self.get_access_token()
        self.site_id = self.get_site_id(site_name)
        self.client = client
        print(f"Initialized Sharepoint with Site name {site_name} and Site ID: {self.site_id}")

    def get_access_token(self):
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
        }
        response = requests.post(self.token_url, data=payload)

        if response.status_code == 200:
            return response.json().get("access_token")
        else:
            raise Exception(f"Failed to obtain token: {response.status_code}, {response.text}")

    def get_site_id(self, site_name):
        url = f"https://graph.microsoft.com/v1.0/sites/{self.graph_hostname}:/sites/{site_name}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            site_id = response.json().get("id")
            if not site_id:
                raise Exception("Site ID not found in the response.")
            return site_id
        else:
            raise Exception(f"Failed to retrieve site ID: {response.status_code}, {response.text}")

    def get_lists_id(self):
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print("Lists fetched successfully")
            return response.json()
        else:
            raise Exception(f"Error fetching lists: {response.status_code}, {response.text}")

    def get_list_data(self, lists, list_name):
        list_dict = {item.get("name"): item.get("id") for item in lists.get("value")}
        list_id = list_dict.get(list_name)
        if not list_id:
            raise Exception(f"List '{list_name}' not found.")

        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{list_id}/items?expand=fields"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        all_items = []

        while url:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                response_json = response.json()
                all_items.extend(response_json.get("value", []))
                url = response_json.get("@odata.nextLink", None)
            else:
                raise Exception(f"Failed to fetch list data: {response.status_code}, {response.text}")
        return all_items
