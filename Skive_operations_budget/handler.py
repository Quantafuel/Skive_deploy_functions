# -*- coding: utf-8 -*-
"""
Created on Wed Mar 26 15:48:01 2025

@author: Espen.Nordsveen
"""


def handle(client, secrets):
    import requests

    # from cog_client import client
    from cognite.client.data_classes import RowWrite

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

        def get_lists_id(self):
            url = f"http://graph.microsoft.com/v1.0/sites/{self.site_id}/lists?$top=100"
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                lists = response.json()
                print("Lists fetched successfully")
                # print(json.dumps(lists, indent=4))
            else:
                raise Exception(f"Error: {response.status_code}, {response.text}")

            return lists

        def get_list_data(self, lists, list_name):
            """
            Fetches all data from a given Microsoft List by name.

            Parameters
            ----------
            lists : DICT
                Structured dict containing all lists for the given site name.
            list_name : STRING
                The name of the list to fetch data from.

            Returns
            -------
            LIST

                Returns a list of all data from the specified list.

            """
            headers = {"Authorization": f"Bearer {self.access_token}"}

            list_dict = {item.get("name"): item.get("id") for item in lists.get("value")}
            # for key, value in list_dict.items():
            #     print(key)
            list_id = list_dict.get(list_name)

            if not list_id:
                print(f"List '{list_name}' not found")

            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{list_id}/items?expand=fields"

            all_items = []
            while url:
                response_list = requests.get(url, headers=headers)
                # print(response_list)
                if response_list.status_code == 200:
                    data = response_list.json()
                    all_items.extend(data.get("value", []))

                    url = data.get("@odata.nextLink")
                    # print(json.dumps(all_items, indent=4))
                else:
                    print(f"Failed to fetch lists: {response_list.status_code}, {response_list.text}")
                    return []
            print(f"Total items fetched from '{list_name}': {len(all_items)}")
            return all_items

        def update_operations_budget_table(self, forecast_list):
            """
            Creates a DataFrame with mapping parameters for sample points from the "Modify Sample Points" list

            Parameters
            ----------
            forecast_list : list
                Contains the response from the get_list_data call

            Returns
            -------
            tb_rows : list
                Returns a list containing operations budget rows

            """

            # data = []
            tb_rows = []
            for entry in forecast_list:
                tb_rows.append(
                    RowWrite(
                        key=entry.get("fields").get("id"),
                        columns={
                            "FinancialYear": entry.get("fields").get("Financial_x0020_Year"),
                            "Month": entry.get("fields").get("Month"),
                            "InfeedWet": entry.get("fields").get("Infeed"),
                            "InfeedDry": entry.get("fields").get("Infeed_x0020_Dry_x0020_Calc"),
                            "Throughput": entry.get("fields").get("Throughput"),
                            "Availability": entry.get("fields").get("Availability"),
                            "OverallUtilization": entry.get("fields").get("OverallUtilization"),
                            "ProducedOil": entry.get("fields").get("ProducedOil"),
                            "OilDryYield": entry.get("fields").get("Oil_x0020_dry_x0020_yield"),
                        },
                    )
                )

            row_list = client.raw.rows.list("production_forecast_db", "operations_budget_tb", limit=None)
            for row in row_list:
                client.raw.rows.delete("production_forecast_db", "operations_budget_tb", row.key)
            client.raw.rows.insert("production_forecast_db", "operations_budget_tb", tb_rows)
            print("Operations budget raw table updated")

            return tb_rows

    Lists = MSListData("S-Skive470")
    lists = Lists.get_lists_id()
    forecast_list = Lists.get_list_data(lists, "OperationsBudget")
    Lists.update_operations_budget_table(forecast_list)
