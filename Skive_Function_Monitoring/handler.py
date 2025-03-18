# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 11:32:28 2025

@author: Henrik.Rost.Breivik
"""


def handle(client, data):

    from datetime import datetime, timedelta

    import pandas as pd
    import yaml

    from cognite.client.data_classes import ExtractionPipelineRun

    # static variables
    functionName = "Function_Monitoring"

    # Global variables with default values
    # extractionPipelineExtId = "Skive_function_call_monitor"
    extractionPipelineExtId = ""
    VIP_Pipe_ExtId = ""
    VIP_functions = []
    data_yconfig = []

    msg = ""
    print(f"[STARTING] {functionName}")
    print("[STARTING] Extracting input data")

    try:
        #
        # read input data from data block extracted from the Extraction Pipeline Configuration
        #

        # Read ExtractionPipelineExtId from the function run time configuration.
        # Need this value to find the function configuration provided as part of the Extraction pipelines
        if "ExtractionPipelineExtId" in data:
            extractionPipelineExtId = data["ExtractionPipelineExtId"]
            print(extractionPipelineExtId)
        else:
            print("[INFO] ExtractionPipelineExtId not found in input function configuration, using default value:")

        # Connect to the Extraction pipeline to read the function configuration
        try:
            pipeline_config_str = client.extraction_pipelines.config.retrieve(extractionPipelineExtId)
            print(pipeline_config_str.config)
            if pipeline_config_str and pipeline_config_str != "":
                data_yconfig = yaml.safe_load(pipeline_config_str.config)["data"]
                print("Data loaded from pipeline")
            else:
                print("Data not loaded from pipeline")
                raise Exception("No configuration found in pipeline")
        except Exception as e:
            print(f"[ERROR] Not able to load pipeline : {extractionPipelineExtId} configuration - {e}")

        # Read the configuration provided as part of the Extraction pipeline configuration
        print(f"[INFO] Config from pipeline: {extractionPipelineExtId} - data: {data_yconfig}")

        if "VIP_functions" in data_yconfig and data_yconfig != []:
            VIP_functions = data_yconfig["VIP_functions"]
            print(f"[INFO] VIP_functions: {VIP_functions}")
        else:
            print(f"[INFO] VIP_functions not found in input configuration in pipeline {extractionPipelineExtId}")

        if "VIP_Pipe_ExtId" in data_yconfig and data_yconfig != []:
            VIP_Pipe_ExtId = data_yconfig["VIP_Pipe_ExtId"]
            print(f"[INFO] VIP_Pipe_ExtId: {VIP_Pipe_ExtId}")
        else:
            print(f"[INFO] VIP_Pipe_ExtId not found in input configuration in pipeline {extractionPipelineExtId}")

        print("[FINISHED] Extracting input parameters")
    except Exception as e:
        print(f"[FAILED] Get state from last run. Error: {e}")
        raise e

    try:
        # ExtPipe = client.extraction_pipelines.retrieve(external_id=extractionPipelineExtId)
        print("Testprint")
        num_calls = 0
        tot_num_fails = 0
        tot_num_success = 0
        local_num_fails = 0
        local_num_success = 0
        results = []

        # Timestamps
        now = datetime.now()
        now_pd_ts = pd.Timestamp(now)
        now_pd_rounded = now_pd_ts.round("min")

        now_rounded_dt = now_pd_rounded.to_pydatetime()
        now_rounded_ts = datetime.timestamp(now_rounded_dt)
        now_rounded_ts_ms = int(now_rounded_ts * 1000)

        td_ago = now_rounded_dt - timedelta(minutes=10)
        td_ago_ts = datetime.timestamp(td_ago)
        td_ago_ts_ms = int(td_ago_ts * 1000)

        time_dict = {"min": td_ago_ts_ms, "max": now_rounded_ts_ms}

        print(f"[START] Monitoring functions calls started between {td_ago} and {now_rounded_dt}")

        functions_list = client.functions.list()

        for i in functions_list._external_id_to_item:
            calls_list = client.functions.calls.list(function_external_id=i, start_time=time_dict, limit=None)
            local_num_fails = 0
            local_num_success = 0

            for k in calls_list._id_to_item:
                call = client.functions.calls.retrieve(call_id=k, function_external_id=i)
                if call.status == "Completed":
                    tot_num_success += 1
                    local_num_success += 1
                if call.status == "Failed":
                    tot_num_fails += 1
                    local_num_fails += 1

                    if i in VIP_functions:
                        print(f"VIP function failed: {i}")
                        # Write error and message back to vip extraction pipeline
                        client.extraction_pipelines.runs.create(
                            ExtractionPipelineRun(status="failure", message=msg, extpipe_external_id=VIP_Pipe_ExtId)
                        )

            results.append([i, local_num_success, local_num_fails])
            num_calls += len(calls_list)

        print(f"Number of function calls: {num_calls}")
        print(f"Number of successes: {tot_num_success}")
        print(f"Number of fails: {tot_num_fails}")

        print(f"[FINISHED] Monitoring functions calls started between {td_ago} and {now_rounded_dt}")
        msg = f"Function: {functionName} - complete. Number of function calls monitored: {num_calls}"
        
        # Write status and message back to extraction pipeline
        client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(status="success", message=msg, extpipe_external_id=extractionPipelineExtId)
        )
    except Exception as e:
        # tb = traceback.format_exc()
        # msg = f"Function: {functionName}: failed - message: {repr(e)} - {tb}"
        msg = f"Function: {functionName} - failed - message:"
        # print(f"[FAILED] {msg}")

        # message sent to the extraction pipeline could only be 1000 char - so make sure it's not longer.
        if len(msg) > 1000:
            msg = msg[0:995] + "..."

        # Write error and message back to extraction pipeline
        client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(status="failure", message=msg, extpipe_external_id=extractionPipelineExtId)
        )
        return {"error": e.__str__(), "status": "failed"}

    print(f"[FINISHED] {functionName} : {msg}")
    # return {"status": "succeeded"}
