def handle(secrets, client):
    #     # import os
    import uuid

    from datetime import datetime, time, timedelta, timezone
    from zoneinfo import ZoneInfo

    import pandas as pd
    import requests

    # from cog_client import client
    from cognite.client.data_classes import Event

    CLIENT_ID = secrets.get("lists-id")
    CLIENT_SECRET = secrets.get("lists-secret")

    # %%
    class DrainData:
        """
        A class to represent drain rounds in Skive. The class fetches data from Sharepoint lists in the Viridor domain

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
            Construct all the necessary attributes for the DrainData object

            Parameters
            ----------
            site_name : str
                name of the sharepoint site to fetch data from

            Returns
            -------
            None.

            """
            self.access_token = DrainData.get_access_token()
            self.site_id = DrainData.get_site_id(site_name)
            self.site_name = site_name
            print(f"Initialized DrainData with Site name {site_name} and Site ID: {self.site_id}")

        def get_lists_id(self):
            url = f"http://graph.microsoft.com/v1.0/sites/{self.site_id}/lists"
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
            list_id = list_dict.get(list_name)
            # print(list_id)
            # site_id = "your-sharepoint-site-id"
            url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{list_id}/items?expand=fields"
            response_list = requests.get(url, headers=headers)
            # print(response_list)
            if response_list.status_code == 200:
                response_json = response_list.json()
                # print(json.dumps(response_json, indent=4))
            else:
                print(f"Failed to fetch lists: {response_list.status_code}, {response_list.text}")

            return response_json.get("value")

        def create_12h_drain_df(self, list_12hours):
            """


            Parameters
            ----------
            list_12hours : list
                List data for 12 hours drain, fetched using get_list_data()

            Returns
            -------
            drain_df : DataFrame
                Returns a dataframe of all drain volumes from the 12 hours drain round with valve tag number as the column header name.

            """

            data = []
            for entry in list_12hours:
                data.append(
                    {
                        "Date": entry.get("fields").get("field_1"),
                        "P03_QJB_QM005": entry.get("fields").get("field_2"),
                        "P04_QJB_QM006": entry.get("fields").get("field_3"),
                        "P02_QJB_QM006": entry.get("fields").get("field_4"),
                        "P01_QJB_QM005": entry.get("fields").get("field_5"),
                        "P10_QJB_QM149": entry.get("fields").get("field_6"),
                        "P10_QEB_QM033": entry.get("fields").get("field_7"),
                    }
                )

            drain_df = pd.DataFrame(data)
            drain_df["Date"] = pd.to_datetime(drain_df["Date"])
            drain_df.set_index("Date", inplace=True)
            drain_df.fillna(0, inplace=True)
            # drain_df["Sum"] = drain_df.sum(axis=1)
            return drain_df

        def create_24h_drain_df(self, list_24hours, list_name):
            """


            Parameters
            ----------
            list_24hours : list
                List data for 24 hours drain, fetched using get_list_data()
            list_name : str
                DESCRIPTION.

            Returns
            -------
            drain_df : TYPE
                DESCRIPTION.

            """
            # list_dict = {item.get("name"): item.get("id") for item in lists.get("value")}
            # list_id = list_dict.get(list_name)

            # headers = {"Authorization": f"Bearer {self.access_token}"}

            # url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/lists/{list_id}/items?expand=fields"
            # response_list = requests.get(url, headers=headers)

            # if response_list.status_code == 200:
            #     response_json = response_list.json()
            #     # print(json.dumps(response_json, indent=4))
            # else:
            #     print(f"Failed to fetch lists: {response_list.status_code}, {response_list.text}")

            data = []
            for entry in list_24hours:
                data.append(
                    {
                        "Date": entry.get("fields").get("field_1"),
                        "P10_QJB_QM150": entry.get("fields").get("field_2"),
                        "P10_EKG_QM039": entry.get("fields").get("field_3"),
                        "P10_QJB_QM148": entry.get("fields").get("field_4"),
                        "P10_EGG_QM139": entry.get("fields").get("field_5"),
                        "P10_EKG_QM031": entry.get("fields").get("field_6"),
                        "P10_GAD_QM007": entry.get("fields").get("field_7"),
                        "P10_GNK_QM729": entry.get("fields").get("field_8"),
                        "P11_GNK_QM955": (
                            entry.get("fields").get("P11_EGG_QM955") if entry.get("fields").get("P11_EGG_QM955") else ""
                        ),
                        "P12_GNK_QM955": (
                            entry.get("fields").get("P12_EGG_QM955") if entry.get("fields").get("P12_EGG_QM955") else ""
                        ),
                    }
                )

            drain_df = pd.DataFrame(data)
            drain_df["Date"] = pd.to_datetime(drain_df["Date"])
            drain_df.set_index("Date", inplace=True)
            drain_df.fillna(0, inplace=True)
            # drain_df["Sum"] = drain_df.sum(axis=1)
            return drain_df

        def single_drain_dp_12h_total(self, drain_list, year, month, day):
            """


            Parameters
            ----------
            drain_list : LIST
                List of drain data for 12h days drain round.
            year : datetime.year
                Input year of interest.
            month : datetime.month
                Input month of interest.
            day : datetime.day
                Input day of interest.

            Returns
            -------
            Returns the total sum of drain volumes for the given date

            """

            drain_sum = 0
            for item in drain_list:
                date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
                # print(datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ"))
                # print(date.day, date.month, date.year, ":", day, month, year)
                if date.day == day and date.month == month and date.year == year:
                    print(item.get("eTag"))
                    drain_sum += (
                        item.get("fields").get("field_2")
                        + item.get("fields").get("field_3")
                        + item.get("fields").get("field_4")
                        + item.get("fields").get("field_5")
                        + item.get("fields").get("field_6")
                        + item.get("fields").get("field_7")
                    )

            return drain_sum, date

        def single_drain_dp_24h_total(self, drain_list, year, month, day):
            """


            Parameters
            ----------
            drain_list : LIST
                List of drain data for 24 hours drain round.
            year : datetime.year
                Input year of interest.
            month : datetime.month
                Input month of interest.
            day : datetime.day
                Input day of interest.

            Returns
            -------
            Returns the total sum of drain volumes for the given date

            """

            drain_sum = 0
            for item in drain_list:
                date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
                # print(datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ"))
                if date.day == day and date.month == month and date.year == year:
                    print(item.get("eTag"))
                    drain_sum += (
                        item.get("fields", {}).get("field_2", 0)
                        + item.get("fields", {}).get("field_3", 0)
                        + item.get("fields", {}).get("field_4", 0)
                        + item.get("fields", {}).get("field_5", 0)
                        + item.get("fields", {}).get("field_6", 0)
                        + item.get("fields", {}).get("field_7", 0)
                        + item.get("fields", {}).get("field_8", 0)
                        + item.get("fields", {}).get("P11_EGG_QM955", 0)
                        + item.get("fields", {}).get("P12_EGG_QM955", 0)
                    )

            return drain_sum, date

        def single_drain_dp_7d_total(self, drain_list, year, month, day):
            """


            Parameters
            ----------
            drain_list : LIST
                List of drain data for 7 days drain round.
            year : datetime.year
                Input year of interest.
            month : datetime.month
                Input month of interest.
            day : datetime.day
                Input day of interest.

            Returns
            -------
            Returns the total sum of drain volumes for the given date

            """
            drain_sum = 0
            for item in drain_list:
                date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
                # print(datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ"))
                if date.day == day and date.month == month and date.year == year:
                    drain_sum += (
                        item.get("fields").get("field_2")
                        + item.get("fields").get("field_3")
                        + item.get("fields").get("field_4")
                        + item.get("fields").get("field_5")
                        + item.get("fields").get("field_6")
                        + item.get("fields").get("field_7")
                        + item.get("fields").get("field_8")
                        + item.get("fields").get("field_9")
                        + item.get("fields").get("field_10")
                        + item.get("fields").get("field_11")
                        + item.get("fields").get("field_12")
                        + item.get("fields").get("field_13")
                        + item.get("fields").get("field_14")
                        + item.get("fields").get("field_15")
                        + item.get("fields").get("field_16")
                        + item.get("fields").get("field_17")
                        + item.get("fields").get("field_18")
                        + item.get("fields").get("field_19")
                        + item.get("fields").get("field_20")
                        + item.get("fields").get("field_21")
                        + item.get("fields").get("field_22")
                        + item.get("fields").get("field_23")
                        + item.get("fields").get("field_24")
                        + item.get("fields").get("field_25")
                        + item.get("fields").get("field_26")
                        + item.get("fields").get("field_27")
                        + item.get("fields").get("field_28")
                        + item.get("fields").get("field_29")
                        + item.get("fields").get("field_30")
                        + item.get("fields").get("field_31")
                        + item.get("fields").get("field_32")
                    )

            return drain_sum, date

    # %% Manual drain
    today = datetime.now(timezone.utc).date()
    t = time(hour=23, minute=59, second=59)
    lastday_end = datetime.combine(today, t) - timedelta(hours=24)
    lastday_end_tz = ZoneInfo("Europe/Oslo")
    yesterday_end = lastday_end.replace(tzinfo=lastday_end_tz)

    drains = DrainData("S-Skive470")

    lists = drains.get_lists_id()
    list_12h = drains.get_list_data(lists, "DrainLog_12hr")
    list_24h = drains.get_list_data(lists, "DrainLog_24hr")
    list_7d = drains.get_list_data(lists, "DrainLog_7d")
    total_drain_12h, date_12h = drains.single_drain_dp_12h_total(
        list_12h, yesterday_end.year, yesterday_end.month, yesterday_end.day
    )
    total_drain_24h, date_24h = drains.single_drain_dp_24h_total(
        list_24h, yesterday_end.year, yesterday_end.month, yesterday_end.day
    )

    drain_events = client.events.list(data_set_ids=665675723108164, limit=None)
    event_times = {datetime.fromtimestamp(event.start_time / 1000) for event in drain_events}

    for item in list_12h:
        drain_sum = 0
        date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
        if date not in event_times:
            # print(datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ"))
            # print(date.day, date.month, date.year, ":", day, month, year)
            drain_sum += (
                (float(item.get("fields").get("field_2")) if item.get("fields").get("field_2") else 0)
                + (float(item.get("fields").get("field_3")) if item.get("fields").get("field_3") else 0)
                + (float(item.get("fields").get("field_4")) if item.get("fields").get("field_4") else 0)
                + (float(item.get("fields").get("field_5")) if item.get("fields").get("field_5") else 0)
                + (float(item.get("fields").get("field_6")) if item.get("fields").get("field_6") else 0)
                + (float(item.get("fields").get("field_7")) if item.get("fields").get("field_7") else 0)
            )
            event = Event(
                external_id="skive_drain_" + str(uuid.uuid4()),
                start_time=date.timestamp() * 1000,
                data_set_id=665675723108164,
                type="Drain data",
                subtype="12_hours_round",
                metadata=({"Total drained volume": drain_sum}),
            )
            print(f"Event for 12 hours drain round created with data {date} - {drain_sum}")
            client.events.create(event)

    for item in list_24h:
        drain_sum = 0
        date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
        if date not in event_times:
            drain_sum += (
                (float(item.get("fields").get("field_2")) if item.get("fields").get("field_2") else 0)
                + (float(item.get("fields").get("field_3")) if item.get("fields").get("field_3") else 0)
                + (float(item.get("fields").get("field_4")) if item.get("fields").get("field_4") else 0)
                + (float(item.get("fields").get("field_5")) if item.get("fields").get("field_5") else 0)
                + (float(item.get("fields").get("field_6")) if item.get("fields").get("field_6") else 0)
                + (float(item.get("fields").get("field_7")) if item.get("fields").get("field_7") else 0)
                + (float(item.get("fields", {}).get("field_8", 0)) if item.get("fields").get("field_8") else 0)
                + (
                    float(item.get("fields", {}).get("P11_EGG_QM955", 0))
                    if item.get("fields").get("P11_EGG_QM955")
                    else 0
                )
                + (
                    float(item.get("fields", {}).get("P12_EGG_QM955", 0))
                    if item.get("fields").get("P12_EGG_QM955")
                    else 0
                )
            )
            event = Event(
                external_id="skive_drain_" + str(uuid.uuid4()),
                start_time=date.timestamp() * 1000,
                data_set_id=665675723108164,
                type="Drain data",
                subtype="24_hours_round",
                metadata=({"Total drained volume": drain_sum}),
            )
            print(f"Event for 24 hours drain round created with data {date} - {drain_sum}")

            client.events.create(event)

    for item in list_7d:
        drain_sum = 0
        date = datetime.strptime(item.get("fields").get("field_1"), "%Y-%m-%dT%H:%M:%SZ")
        if date not in event_times:
            drain_sum += (
                (item.get("fields").get("field_2") if item.get("fields").get("field_2") else 0)
                + (item.get("fields").get("field_3") if item.get("fields").get("field_3") else 0)
                + (item.get("fields").get("field_4") if item.get("fields").get("field_4") else 0)
                + (item.get("fields").get("field_5") if item.get("fields").get("field_5") else 0)
                + (item.get("fields").get("field_6") if item.get("fields").get("field_6") else 0)
                + (item.get("fields").get("field_7") if item.get("fields").get("field_7") else 0)
                + (item.get("fields").get("field_8") if item.get("fields").get("field_8") else 0)
                + (item.get("fields").get("field_9") if item.get("fields").get("field_9") else 0)
                + (item.get("fields").get("field_10") if item.get("fields").get("field_10") else 0)
                + (item.get("fields").get("field_11") if item.get("fields").get("field_11") else 0)
                + (item.get("fields").get("field_12") if item.get("fields").get("field_12") else 0)
                + (item.get("fields").get("field_13") if item.get("fields").get("field_13") else 0)
                + (item.get("fields").get("field_14") if item.get("fields").get("field_14") else 0)
                + (item.get("fields").get("field_15") if item.get("fields").get("field_15") else 0)
                + (item.get("fields").get("field_16") if item.get("fields").get("field_16") else 0)
                + (item.get("fields").get("field_17") if item.get("fields").get("field_17") else 0)
                + (item.get("fields").get("field_18") if item.get("fields").get("field_18") else 0)
                + (item.get("fields").get("field_19") if item.get("fields").get("field_19") else 0)
                + (item.get("fields").get("field_20") if item.get("fields").get("field_20") else 0)
                + (item.get("fields").get("field_21") if item.get("fields").get("field_21") else 0)
                + (item.get("fields").get("field_22") if item.get("fields").get("field_22") else 0)
                + (item.get("fields").get("field_23") if item.get("fields").get("field_23") else 0)
                + (item.get("fields").get("field_24") if item.get("fields").get("field_24") else 0)
                + (item.get("fields").get("field_25") if item.get("fields").get("field_25") else 0)
                + (item.get("fields").get("field_26") if item.get("fields").get("field_26") else 0)
                + (item.get("fields").get("field_27") if item.get("fields").get("field_27") else 0)
                + (item.get("fields").get("field_28") if item.get("fields").get("field_28") else 0)
                + (item.get("fields").get("field_29") if item.get("fields").get("field_29") else 0)
                + (item.get("fields").get("field_30") if item.get("fields").get("field_30") else 0)
                + (item.get("fields").get("field_31") if item.get("fields").get("field_31") else 0)
                + (item.get("fields").get("field_32") if item.get("fields").get("field_32") else 0)
            )

            event = Event(
                external_id="skive_drain_" + str(uuid.uuid4()),
                start_time=date.timestamp() * 1000,
                data_set_id=665675723108164,
                type="Drain data",
                subtype="7_days_round",
                metadata=({"Total drained volume": drain_sum}),
            )
            print(f"Event for 7 days drain round created with data {date} - {drain_sum}")
            client.events.create(event)
