# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 14:59:54 2024

@author: Espen.Nordsveen
"""


def handle(client):

    import csv

    import pandas as pd

    from cognite.client.data_classes import RowWrite

    def detect_delimiter(sample_line):
        """
        Try to guess the delimiter using csv.Sniffer.
        Falls back on a simple heuristic.
        Considers tab, semicolon and comma.
        """
        try:
            dialect = csv.Sniffer().sniff(sample_line, delimiters="\t;,")
            return dialect.delimiter
        except Exception:
            # Fallback: whichever is more common:
            if sample_line.count("\t") >= sample_line.count(";"):
                return "\t"
            elif sample_line.count(";") >= sample_line.count(","):
                return ";"
            else:
                return ","

    files = client.files.list(data_set_ids=5883851598140017, limit=None)

    for file in files:
        print(file.name)
        file_content = client.files.download_bytes(id=file.id)
        content = file_content.decode("utf-8", errors="replace")

        sections = content.split("Analysis Calculation Info")
        # analysis_section = sections[0]
        remaining_sections = sections[1].split("Alkane Profile")

        delim = detect_delimiter(sections[0])
        calculation_section = remaining_sections[0].replace(delim, ";")
        metadata_section = sections[0].replace(delim, ";")
        alkane_profile_section = remaining_sections[1].replace(delim, ";")

        # Find the Sample time from metadata
        for line in metadata_section.splitlines():
            line = line.replace('"', "")
            # print(line)
            if "InjectionTime" in line:
                date = line.split(";")[1]
                print(date)
            # match_time = re.search(r'"InjectionTime;""(.*?)""', line)
            # match_time =  re.search(r'"InjectionTime;("+) (.+?) \1', line, re.VERBOSE)
            # print(match_time)
            # match_id = re.search(r'"SampleName;""(.*?)""+"', line)
            # match_id = re.search(r'"SampleName";\s*"([^"]+)"', line)
            # match_id = re.search(r'"SampleName;("+) (.+?) \1', line, re.VERBOSE)
            if "SampleName" in line:
                sample_id = line.split(";")[1].split("_")[0]
                print(sample_id)
            # # print(match_id)
            # if match_time:
            #     injection_time = match_time.group(1)
            #     date = parser.parse(injection_time).isoformat()
            # if match_id:
            #     sample_id = match_id.group(1).split("_")[0]
        # Find the flashpoint value from the alkane profile section
        for line in alkane_profile_section.splitlines():
            line = line.replace('"', "")
            if "Flashpoint" in line:
                flashpoint_value = line.split(";")[1]
            # match_fp = re.search(r'"ASTM D\d+ Flashpoint";(-?\d+\.\d+)', line)
            # if match_fp:
            #     flashpoint_value = match_fp.group(1)

        calculation_lines = calculation_section.splitlines()[1:]  # Skip the header
        calculation_data = []
        for line in calculation_lines:
            calculation_data.append([item.strip('"') for item in line.split(";")])

        # Convert to DataFrame
        # calculation_df = pd.DataFrame(calculation_data)
        data_split = alkane_profile_section.split("\r\n\r\n\r\n")
        alkane_lines = data_split[0].splitlines()
        cutpoint_lines = data_split[1].splitlines()
        distribution_lines = data_split[2].splitlines()
        columns_alkane = [col.strip('"') for col in alkane_lines[1].split(";")]
        columns_cutpoint = [col.strip('"') for col in cutpoint_lines[1].split(";")]
        columns_distribution = [col.strip('"') for col in distribution_lines[1].split(";")]
        columns_cutpoint.pop(-1)
        columns_distribution.pop(-1)
        alkane_data = []
        cutpoint_data = []
        distribution_data = []

        for line in alkane_lines[1:]:
            alkane_data.append([item.replace(",", ".").strip('"') for item in line.split(";")])
        for line in cutpoint_lines[1:]:
            cutpoint_data.append([item.replace(",", ".").strip('"') for item in line.split(";")])
        for line in distribution_lines[1:]:
            distribution_data.append([item.replace(",", ".").strip('"') for item in line.split(";")])

        cutpoint_data = [row[:-1] if len(row) > 1 else row for row in cutpoint_data]
        distribution_data = [row[:-1] if len(row) > 1 else row for row in distribution_data]

        alkane_df = pd.DataFrame(alkane_data, columns=columns_alkane)
        alkane_df = alkane_df.drop(["BP", "n-Paraffins", "Unknowns"], axis=1).transpose()
        alkane_df.columns = ["C" + col.replace('"', "") for col in alkane_df.iloc[0]]
        alkane_df = alkane_df[1:]
        alkane_df = alkane_df.reset_index().drop(["CCarbon", "index", "CTotals", "CReport End"], axis=1)

        cutpoint_df = pd.DataFrame(cutpoint_data, columns=columns_cutpoint)
        cutpoint_df = cutpoint_df[2:]
        cutpoint_df = cutpoint_df.transpose()
        cutpoint_df.columns = ["recovered_at_" + col.replace(",0", "").replace(".0", "") for col in cutpoint_df.iloc[0]]
        cutpoint_df = cutpoint_df[1:]
        cutpoint_df.columns = cutpoint_df.columns.str.strip().str.replace("\r", "", regex=False)
        cutpoint_df = cutpoint_df.reset_index().drop(["index", "recovered_at_Report End"], axis=1)
        # cutpoint_df = cutpoint_df.reset_index().drop(["index", "recovered_at_°C", "recovered_at_BP", "recovered_at_Report End"], axis=1)

        distribution_df = pd.DataFrame(distribution_data, columns=columns_distribution)
        idx = 0
        for i in distribution_df.iterrows():
            if "Report End" in i[1][0]:
                # print(i)
                idx = i[0]
                break
        distribution_df = distribution_df[2:idx].reset_index().drop(["index"], axis=1).transpose()
        distribution_df.columns = ["T" + col.replace(",0", "").replace(".0", "") for col in distribution_df.iloc[0]]
        distribution_df = distribution_df[1:]
        distribution_df = distribution_df.reset_index().drop(["index"], axis=1)
        rename_map = {}
        if "T0,5" in distribution_df.columns:
            rename_map["T0,5"] = "IBP"
        elif "T0.5" in distribution_df.columns:
            rename_map["T0.5"] = "IBP"
        if "T99,5" in distribution_df.columns:
            rename_map["T99,5"] = "FBP"
        elif "T99.5" in distribution_df.columns:
            rename_map["T99.5"] = "FBP"
        distribution_df = distribution_df.rename(columns=rename_map)

        cutpoint_columns_to_remove = []
        if "recovered_at_250" in cutpoint_df.columns:
            cutpoint_columns_to_remove.append("recovered_at_250")
        if "recovered_at_350" in cutpoint_df.columns:
            cutpoint_columns_to_remove.append("recovered_at_350")

        row_list = client.raw.rows.list("lab_db", "simdist_tb", limit=None)
        last_key = 1
        new_key = 1
        try:
            for row in row_list:
                if last_key <= int(row.key):
                    last_key = int(row.key)
                    new_key = last_key + 1
        except Exception as e:
            print("No rows in table:", e)
            pass

        df_raw = pd.concat(
            [
                pd.DataFrame(data={"SampleID": sample_id}, index=[0]),
                pd.DataFrame(data={"key": new_key}, index=[0]),
                pd.DataFrame(data={"C60": ""}, index=[0]),
                pd.DataFrame(data={"Date": date}, index=[0]),
                distribution_df,
                cutpoint_df[cutpoint_columns_to_remove],
                pd.DataFrame(data={"Flash_point": flashpoint_value}, index=[0]),
                alkane_df,
            ],
            axis=1,
        )
        lab_data_df_existing = client.raw.rows.retrieve_dataframe("lab_db", "simdist_tb", limit=None)
        common_cols = df_raw.columns.intersection(lab_data_df_existing.columns)
        compare_cols = [col for col in common_cols if col != "key"]
        row_to_check = df_raw[compare_cols].iloc[0]
        row_exists = (lab_data_df_existing[compare_cols] == row_to_check[compare_cols]).all(axis=1).any()
        # row_exists = (lab_data_df_existing[common_cols] == row_to_check).all(axis=1).any()

        values = df_raw.values.tolist()[0]
        columns = df_raw.columns.tolist()

        row_dict = dict(zip(columns, values))
        rows = [RowWrite(key=new_key, columns=row_dict)]
        # print(df_raw)
        if not row_exists:
            client.raw.rows.insert("lab_db", "simdist_tb", rows)
            print(f"Sample {sample_id} inserted into table")
        else:
            print("Sample already in table")
