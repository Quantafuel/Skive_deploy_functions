def handle(secrets, client):
    """
    [requirements]
    uuid
    datetime
    requests
    numpy
    [/requirements]
    """
    import uuid

    from datetime import datetime, timedelta

    import requests

    from cognite.client.data_classes import Event

    class MSListData:
        """
        A class to represent data from MS Lists in Skive. The class fetches data from Sharepoint lists in the Viridor domain

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
        CLIENT_ID = secrets.get("lists-id")
        CLIENT_SECRET = secrets.get("lists-secret")
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
            self.access_token = MSListData.get_access_token()
            self.site_id = MSListData.get_site_id(site_name)
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

        def create_line_narrative(self, production_status_list):
            events = client.events.list(
                data_set_ids=6574982093948393, limit=None
            )  # , start_time={"min": int(start_of_day.timestamp()*1000)})

            for entry in production_status_list:
                date = datetime.strptime(entry.get("fields").get("Created"), "%Y-%m-%dT%H:%M:%SZ")
                # if date.day == today.day and date.month == today.month and date.year == today.year:
                print(date)
                print("Event timestamp:", date)
                print("Last stored event timestamp:", datetime.fromtimestamp(events.data[-1].start_time / 1000))
                if events.data[-1].start_time < int(date.timestamp() * 1000):

                    print("One new event")
                    data_event = Event(
                        external_id="skive_line_status-" + str(uuid.uuid4()),
                        data_set_id=6574982093948393,
                        start_time=int(date.timestamp() * 1000),
                        type="Line status",
                        subtype="raw",
                        description="Calculation of last days production data, based on counter data",
                        metadata=(
                            {
                                "L1 Status": (
                                    entry.get("fields").get("L1Status") if entry.get("fields").get("L1Status") else ""
                                ),
                                "L2 Status": (
                                    entry.get("fields").get("L2Status") if entry.get("fields").get("L2Status") else ""
                                ),
                                "L3 Status": (
                                    entry.get("fields").get("L3Status") if entry.get("fields").get("L3Status") else ""
                                ),
                                "L4 Status": (
                                    entry.get("fields").get("L4Status") if entry.get("fields").get("L4Status") else ""
                                ),
                                "Overall status": (
                                    entry.get("fields").get("OverallStatus")
                                    if entry.get("fields").get("OverallStatus")
                                    else ""
                                ),
                            }
                        ),
                    )
                    client.events.create(data_event)

    qf_ms_lists = MSListData("S-Skive470")
    lists = qf_ms_lists.get_lists_id()
    line_status_list = qf_ms_lists.get_list_data(lists, "LineStatus")
    qf_ms_lists.create_line_narrative(line_status_list)

    vacuum_line1_latest_dp = client.time_series.data.retrieve_latest(
        external_id="2s=P01_PRES_CONT_TEXT_ACTIVE_MODE", before="now"
    ).value[0]
    vacuum_line1_latest_time = client.time_series.data.retrieve_latest(
        external_id="2s=P01_PRES_CONT_TEXT_ACTIVE_MODE", before="now"
    ).timestamp[0]
    vacuum_line2_latest_dp = client.time_series.data.retrieve_latest(
        external_id="2s=P02_PRES_CONT_TEXT_ACTIVE_MODE", before="now"
    ).value[0]
    vacuum_line2_latest_time = client.time_series.data.retrieve_latest(
        external_id="2s=P02_PRES_CONT_TEXT_ACTIVE_MODE", before="now"
    ).timestamp[0]
    vacuum_line3_latest_dp = client.time_series.data.retrieve_latest(
        external_id="2s=P03_PRES_CONT_TEXT_ACTIVE_MODE", before="now"
    ).value[0]
    vacuum_line3_latest_time = client.time_series.data.retrieve_latest(
        external_id="2s=P03_PRES_CONT_TEXT_ACTIVE_MODE", before="now"
    ).timestamp[0]
    vacuum_line4_latest_dp = client.time_series.data.retrieve_latest(
        external_id="2s=P04_PRES_CONT_TEXT_ACTIVE_MODE", before="now"
    ).value[0]
    vacuum_line4_latest_time = client.time_series.data.retrieve_latest(
        external_id="2s=P04_PRES_CONT_TEXT_ACTIVE_MODE", before="now"
    ).timestamp[0]

    feed_screw_line1 = client.time_series.data.retrieve_latest(
        external_id="2s=P01EAC01GL001M101:M_HAST", before="now"
    ).value[0]
    feed_screw_line2 = client.time_series.data.retrieve_latest(
        external_id="2s=P02EAC02GL001M201:M_HAST", before="now"
    ).value[0]
    feed_screw_line3 = client.time_series.data.retrieve_latest(
        external_id="2s=P03EAC03GL001M301:M_HAST", before="now"
    ).value[0]
    feed_screw_line4 = client.time_series.data.retrieve_latest(
        external_id="2s=P04EAC04GL001M401:M_HAST", before="now"
    ).value[0]

    temp_line1 = client.time_series.data.retrieve_arrays(
        external_id="2s=P01ECC01TR111:M_MID", start=datetime.now() - timedelta(minutes=4), end=datetime.now()
    ).value
    temp_line2 = client.time_series.data.retrieve_arrays(
        external_id="2s=P02ECC02TR211:M_MID", start=datetime.now() - timedelta(minutes=4), end=datetime.now()
    ).value
    temp_line3 = client.time_series.data.retrieve_arrays(
        external_id="2s=P03ECC03TR311:M_MID", start=datetime.now() - timedelta(minutes=4), end=datetime.now()
    ).value
    temp_line4 = client.time_series.data.retrieve_arrays(
        external_id="2s=P04ECC04TR411:M_MID", start=datetime.now() - timedelta(minutes=4), end=datetime.now()
    ).value

    vacuum_line_list = [vacuum_line1_latest_dp, vacuum_line2_latest_dp, vacuum_line3_latest_dp, vacuum_line4_latest_dp]
    vacuum_line_time_list = [
        vacuum_line1_latest_time,
        vacuum_line2_latest_time,
        vacuum_line3_latest_time,
        vacuum_line4_latest_time,
    ]
    feed_screw_list = [feed_screw_line1, feed_screw_line2, feed_screw_line3, feed_screw_line4]
    temp_list = [temp_line1, temp_line2, temp_line3, temp_line4]
    line1_status, line2_status, line3_status, line4_status = ("Shutdown", "Shutdown", "Shutdown", "Shutdown")
    line_list = [line1_status, line2_status, line3_status, line4_status]

    i = 0
    time_now_ms = datetime.now().timestamp() * 1000
    four_minutes_ms = 4 * 60 * 1000
    status_dict = {"STANDSTILL": 1, "INERTING": 2, "STANDBY": 3, "COOLDOWN": 4, "STARTUP": 5, "RUNNING": 6}

    for pump in vacuum_line_list:
        if pump == 1:
            print("pump = 1")
            if feed_screw_list[i] < 2 and temp_list[i][-1] > 200:
                line_list[i] = "STANDBY"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["STANDBY"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
            elif feed_screw_list[i] < 2 and temp_list[i][-1] < 50:
                print("low temp, screw speed < 2")
                line_list[i] = "STANDSTILL"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["STANDSTILL"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
        elif pump == 2:
            print("pump = 2")
            if (
                feed_screw_list[i] < 2
                and temp_list[i][-1] > 200
                and abs(time_now_ms - vacuum_line_time_list[i]) < four_minutes_ms
            ):
                line_list[i] = "STANDBY"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["STANDBY"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
            elif feed_screw_list[i] > 2 and temp_list[i][-1] > 200:
                line_list[i] = "RUNNING"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["RUNNING"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
            elif (
                feed_screw_list[i] < 2
                and temp_list[i][-1] < 50
                and abs(time_now_ms - vacuum_line_time_list[i]) > four_minutes_ms
            ):
                line_list[i] = "STANDBY"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["STANDBY"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
        elif pump == 3:
            print("pump = 3")
            if feed_screw_list[i]:
                line_list[i] = "INERTING"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["INERTING"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
        elif pump == 4:
            print("pump = 4")
            if (
                feed_screw_list[i] < 2
                and (temp_list[i][-1] - temp_list[i][0]) < -5
                and abs(time_now_ms - vacuum_line_time_list[i]) > four_minutes_ms
            ):
                line_list[i] = "COOLDOWN"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["COOLDOWN"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
            elif (
                feed_screw_list[i] < 2
                and (temp_list[i][-1] - temp_list[i][0]) > 5
                and abs(time_now_ms - vacuum_line_time_list[i]) > four_minutes_ms
            ):
                line_list[i] = "STARTUP"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["STARTUP"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
            elif (
                feed_screw_list[i] < 2
                and temp_list[i][-1] < 50
                and abs(time_now_ms - vacuum_line_time_list[i]) > four_minutes_ms
            ):
                line_list[i] = "COOLDOWN"
                client.time_series.data.insert(
                    [(datetime.now(), status_dict["COOLDOWN"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
                )
        elif pump == 5:
            print("pump = 5", pump)
            line_list[i] = "RUNNING"
            client.time_series.data.insert(
                [(datetime.now(), status_dict["RUNNING"])], external_id="LIVE_STATUS_LINE_" + str(i + 1)
            )
        i += 1
