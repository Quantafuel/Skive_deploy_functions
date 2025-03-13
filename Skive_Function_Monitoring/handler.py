# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 11:32:28 2025

@author: Henrik.Rost.Breivik
"""

import logging
import traceback

# import arrow
# import numpy as np
from datetime import datetime, timedelta

import pandas as pd
import yaml

from cognite.client.data_classes import ExtractionPipelineRun
from cognite.logger import configure_logger


# Configure application logger (only done ONCE):
configure_logger(logger_name="func", log_json=False, log_level="INFO")

# The following line must be added to all python modules (after imports):
logger = logging.getLogger(f"func.{__name__}")
logger.info("---------------------------------------START--------------------------------------------")

# static variables
functionName = "Function Monitor"

# Global variables with default values
extractionPipelineExtId = "Skive_function_call_monitor"
VIP_functions = []


def handle(client, data):
    msg = ""
    logger.info(f"[STARTING] {functionName}")
    logger.info(f"[INFO] Cognite Client login status: {client.login.status()}")

    logger.info("[STARTING] Extracting input data")
    try:
        get_input_data(client, data)
        logger.info("[FINISHED] Extracting input parameters")
    except Exception as e:
        logger.error(f"[FAILED] Get state from last run. Error: {e}")
        raise e

    try:
        # Calculate new output / time series data points based on input data
        # startDate, num_points = sine_calcutation(client)
        res = monitor_function(client)
        msg = f"Function: {functionName}: complete"

        print(res)

        # Write status and message back to extraction pipeline
        client.extraction_pipeline_runs.create(
            ExtractionPipelineRun(status="success", message=msg, external_id=extractionPipelineExtId)
        )
    except Exception as e:
        tb = traceback.format_exc()
        msg = f"Function: {functionName}: failed - message: {repr(e)} - {tb}"
        logger.info(f"[FAILED] {msg}")
        # message sent to the extraction pipeline could only be 1000 char - so make sure it's not longer.
        if len(msg) > 1000:
            msg = msg[0:995] + "..."

        # Write error and message back to extraction pipeline
        client.extraction_pipeline_runs.create(
            ExtractionPipelineRun(status="failure", message=msg, external_id=extractionPipelineExtId)
        )
        return {"error": e.__str__(), "status": "failed"}

    logger.info(f"[FINISHED] {functionName} : {msg}")

    return {"status": "succeeded"}


#
# read input data from data block extracted from the Extraction Pipeline Configuration
#
def get_input_data(client, data):
    global extractionPipelineExtId, VIP_functions

    # Read ExtractionPipelineExtId from the function run time configuration.
    # Need this value to find the function configuration provided as part of the Extraction pipelines
    if "ExtractionPipelineExtId" in data:
        extractionPipelineExtId = data["ExtractionPipelineExtId"]
    else:
        logger.info("[INFO] ExtractionPipelineExtId not found in input function configuration, using default value:")

    # Connect to the Extraction pipeline to read the function configuration
    try:
        pipeline_config_str = client.extraction_pipelines.config.retrieve(extractionPipelineExtId)
        if pipeline_config_str and pipeline_config_str != "":
            data = yaml.safe_load(pipeline_config_str.config)["data"]
        else:
            raise Exception("No configuration found in pipeline")
    except Exception as e:
        logger.error(f"[ERROR] Not able to load pipeline : {extractionPipelineExtId} configuration - {e}")

    # Read the configuration provided as part of the Extraction pipeline configuration
    logger.info(f"[INFO] Config from pipeline: {extractionPipelineExtId} - data: {data}")

    if "VIP_functions" in data:
        VIP_functions = data["VIP_functions"]
        logger.info(f"[INFO] VIP_functions: {VIP_functions}")
    else:
        logger.info(f"[INFO] VIP_functions not found in input configuration in pipeline {extractionPipelineExtId}")


#
#  monitoring function -
#
def monitor_function(client):

    # Get the data set ID, used to connect the time series created
    # ExtPipe = client.extraction_pipelines.retrieve(external_id=extractionPipelineExtId)

    results = []
    results.append(0)

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
    print(time_dict)

    logger.info(f"[START] Monitoring functions calls started between {now_rounded_dt} and {td_ago}")

    # logger.info(
    #    f"[INFO] For calculation use, amplitude: {amplitude} (cure hight) and periods: {period} (number of curves)"
    # )

    # Create time series
    # Note: we delete the time_series in this example just to keep it simple to view and test the function.
    # client.time_series.create(
    #    TimeSeries(
    #        name=ts_name,
    #        external_id=ts_external_id,
    #        description="Function sine_wave generated time series",
    #        data_set_id=ExtPipe.data_set_id,
    #    )
    # )

    logger.info(f"[FINISHED] Monitoring functions calls started between {now_rounded_dt} and {td_ago}")

    return results
