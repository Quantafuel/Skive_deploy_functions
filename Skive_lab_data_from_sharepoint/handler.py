# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 14:01:28 2024

@author: Espen.Nordsveen
"""


def handle(secrets, client):
    import pandas as pd
    import requests

    # from cog_client import client

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


            Parameters
            ----------
            lists : DICT
                Structured dict containing all lists for the given site name.
            list_name : STRING
                The name of the list to fetch data from.

            Returns
            -------
            LIST
                Returns list of data from the given list name

            """
            headers = {"Authorization": f"Bearer {self.access_token}"}

            list_dict = {item.get("name"): item.get("id") for item in lists.get("value")}
            # for key, value in list_dict.items():
            #     print(key)
            list_id = list_dict.get(list_name)
            all_items = []
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{list_id}/items?$expand=fields"

            while url:
                response_list = requests.get(url, headers=headers)
                # print(response_list)
                if response_list.status_code == 200:
                    response_json = response_list.json()
                    items = response_json.get("value", [])
                    all_items.extend(items)
                    url = response_json.get("@odata.nextLink", None)
                else:
                    print(f"Failed to fetch lists: {response_list.status_code}, {response_list.text}")

            return all_items

        def sample_points_mapping(self, mapping_list):
            """
            Creates a DataFrame with mapping parameters for sample points from the "Modify Sample Points" list

            Parameters
            ----------
            mapping_list : list
                Contains the response from the sample point list for Lab.

            Returns
            -------
            sample_df : DataFrame
                Returns a Pandas DataFrame containing the mapping parameters.

            """
            data = []
            for entry in mapping_list:
                data.append(
                    {
                        "SamplePointId": entry.get("fields").get("SamplePointID"),
                        "LinkTitle": entry.get("fields").get("LinkTitle"),
                        "Description": entry.get("fields").get("Description"),
                    }
                )
            sample_points_df = pd.DataFrame(data)

            sample_points_df.fillna(0, inplace=True)

            client.raw.rows.insert_dataframe("lab_db", "sample_points_tb", sample_points_df)
            print("Sample points table inserted in raw")
            return sample_points_df

        def test_types_mapping(self, mapping_list):
            """

            Creates a DataFrame with mapping parameters for test types from the "Modify Test Types" list

            Parameters
            ----------
            mapping_list : list
                Contains the response from the test types list for Lab.

            Returns
            -------
            sample_df : DataFrame
                Returns a Pandas DataFrame containing the mapping parameters.

            """
            data = []
            for entry in mapping_list:
                data.append(
                    {
                        "Id": entry.get("fields").get("id"),
                        "TestType": entry.get("fields").get("Title"),
                        "TestTypeCode": entry.get("fields").get("TestTypeCode"),
                    }
                )
            test_types_df = pd.DataFrame(data)

            test_types_df.fillna(0, inplace=True)
            client.raw.rows.insert_dataframe("lab_db", "test_types_tb", test_types_df)
            print("Test types table inserted in raw")
            return test_types_df

        def get_user_name(self, user_id):
            """Fetches user name from Microsoft Graph API using user ID."""
            url = f"https://graph.microsoft.com/v1.0/users/{user_id}"
            headers = {"Authorization": f"Bearer {self.access_token}"}

            response = requests.get(url, headers=headers)
            print(response.json())
            if response.status_code == 200:
                return response.json().get("displayName", "Unknown User")
            else:
                return "Unknown User"

        def sample_list_data(self, sample_list):
            """

            Parameters
            ----------
            sample_list : list


            Returns
            -------
            sample_df : DataFrame
                Returns a dataframe of sample list data, mapped against test types and samples

            """
            lists = LabLists.get_lists_id()

            sample_points_list = LabLists.get_list_data(lists, "SamplePoints")
            sample_mapping = LabLists.sample_points_mapping(sample_points_list)
            test_type_list = LabLists.get_list_data(lists, "TestTypes")
            test_types_mapping = LabLists.test_types_mapping(test_type_list)

            sample_mapping_dict = dict(zip(sample_mapping["SamplePointId"], sample_mapping["LinkTitle"]))

            sample_mapping_dict = {str(int(k)) if k.isdigit() else k: v for k, v in sample_mapping_dict.items()}
            sample_mapping_description_dict = dict(zip(sample_mapping["SamplePointId"], sample_mapping["Description"]))
            sample_mapping_description_dict = {
                str(int(k)) if k.isdigit() else k: v for k, v in sample_mapping_description_dict.items()
            }
            test_types_mapping_dict = dict(zip(test_types_mapping["Id"], test_types_mapping["TestType"]))

            data = []
            for entry in sample_list:
                data.append(
                    {
                        "Date": entry.get("fields").get("Sampletime"),
                        "ID": entry.get("fields").get("id"),
                        "Sampler": entry.get("fields").get("SAmplerLookupId"),
                        "TestType": entry.get("fields").get("TestType1LookupId"),
                        "SamplePoint": entry.get("fields").get("SamplePointLookupId"),
                        "SamplePointDescription": entry.get("fields").get("SamplePoint_x003a_DescriptionLookupId"),
                        "Recept": entry.get("fields").get("Recept"),
                        "Status": entry.get("fields").get("Status"),
                    }
                )

            sample_df = pd.DataFrame(data)
            # sample_df["Date"] = pd.to_datetime(sample_df["Date"]).dt.tz_localize(None)
            # sample_df["Date"] = sample_df["Date"].apply(lambda x: x.timestamp() if pd.notnull(x) else 0)
            # sample_df.set_index("Date", inplace=True)
            sample_df.reset_index(drop=True, inplace=True)
            # sample_df.index = range(len(sample_df))
            sample_df.fillna(0, inplace=True)
            # drain_df["Sum"] = drain_df.sum(axis=1)
            sample_df["SamplePoint"] = sample_df["SamplePoint"].map(sample_mapping_dict)
            sample_df["TestType"] = sample_df["TestType"].map(test_types_mapping_dict)
            sample_df["SamplePointDescription"] = sample_df["SamplePointDescription"].map(
                sample_mapping_description_dict
            )
            client.raw.rows.insert_dataframe("lab_db", "samples_list_tb", sample_df)
            print("Sample list table inserted in raw")
            return sample_df

        def manual_analysis_data(self, sample_list_df):
            """

            Parameters
            ----------
            sample_list_df : DataFrame


            Returns
            -------
            manual_analysis_df : DataFrame
                Returns a dataframe of manual analysis data directly from the MS list, and mapped against sample_list_df

            """
            lists = LabLists.get_lists_id()

            manual_analysis_list = LabLists.get_list_data(lists, "Manual Results List")
            # sample_mapping = LabLists.sample_points_mapping(manual_analysis_list)
            # test_type_list = LabLists.get_list_data(lists, "TestTypes")
            # test_types_mapping = LabLists.test_types_mapping(test_type_list)

            # sample_mapping_dict = dict(zip(sample_mapping['SamplePointId'], sample_mapping['LinkTitle']))

            # sample_mapping_dict = {str(int(k)) if k.isdigit() else k: v for k, v in sample_mapping_dict.items()}
            # sample_mapping_description_dict = dict(zip(sample_mapping['SamplePointId'], sample_mapping['Description']))
            # sample_mapping_description_dict = {str(int(k)) if k.isdigit() else k: v for k, v in sample_mapping_description_dict.items()}
            # test_types_mapping_dict = dict(zip(test_types_mapping['Id'], test_types_mapping['TestType']))
            # print(manual_analysis_list)
            data = []
            for entry in manual_analysis_list:
                data.append(
                    {
                        "ID": entry.get("fields").get("id"),
                        "SampleID": entry.get("fields").get("SampleIDLookupId"),
                        "Viscosity": entry.get("fields").get("Viscosity"),
                        "Color": entry.get("fields").get("Color"),
                        "Particles": entry.get("fields").get("Particles"),
                        "Density": entry.get("fields").get("Density"),
                        "MeltingPoint": entry.get("fields").get("MeltingPoint"),
                        "pH_Balance": entry.get("fields").get("pH_x0020_Balance"),
                        "FlashPoint": entry.get("fields").get("FlashPoint"),
                        "Sulphur": entry.get("fields").get("Sulphur"),
                        "Chlorine": entry.get("fields").get("Chlorine"),
                        "LowerHeatingValue": entry.get("fields").get("LowerHeatingValue"),
                        "Comment": entry.get("fields").get("Comment"),
                        "Created_by": entry.get("fields").get("AuthorLookupId"),
                    }
                )

            manual_analysis_df = pd.DataFrame(data)
            # manual_analysis_df["Date"] = pd.to_datetime(manual_analysis_df["Date"])
            # manual_analysis_df.set_index("Date", inplace=True)
            manual_analysis_df.fillna(0, inplace=True)

            # manual_analysis_df.reset_index(inplace=True)
            # manual_analysis_df["SampleTime"] = manual_analysis_df["SampleTime"].map(sample_list_df.set_index("ID")["Date"])
            # # drain_df["Sum"] = drain_df.sum(axis=1)
            # sample_df["SamplePoint"] = sample_df["SamplePoint"].map(sample_mapping_dict)
            # sample_df["TestType"] = sample_df["TestType"].map(test_types_mapping_dict)
            # sample_df["SamplePointDescription"] = sample_df["SamplePointDescription"].map(sample_mapping_description_dict)
            client.raw.rows.insert_dataframe("lab_db", "manual_results_tb", manual_analysis_df)
            print("Manual analysis table inserted in raw")
            return manual_analysis_df

    LabLists = MSListData("S-Skive470")

    lists = LabLists.get_lists_id()
    sample_list = LabLists.get_list_data(lists, "LAB_Test")
    sample_list_df = LabLists.sample_list_data(sample_list)
    LabLists.manual_analysis_data(sample_list_df)
