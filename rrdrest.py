import logging
from fastapi import FastAPI, HTTPException
from backend.RRD_parse import RRD_parser
from typing import Optional, Dict, Any
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

rrd_rest = FastAPI(
    title="RRDReST",
    description="Makes RRD files API-able",
    version="0.4",  # Updated version
)

# Define the process pool
executor = ProcessPoolExecutor(max_workers=4)

def process_rrd_file(individual_rrd_path: str, epoch_start_time: Optional[int], epoch_end_time: Optional[int], port_id: str) -> Dict[str, Any]:
    """Function to process an individual RRD file"""
    result = {}
    if os.path.isfile(individual_rrd_path):
        try:
            rr = RRD_parser(
                rrd_file=individual_rrd_path,
                start_time=epoch_start_time,
                end_time=epoch_end_time
            )
            r = rr.compile_result()

            # Add the port_id (instead of port-id) to the data
            if "data" in r:
                for entry in r["data"]:
                    entry["port_id"] = port_id  # Use port_id with the actual port id value

            result[individual_rrd_path] = r
        except Exception as e:
            result[individual_rrd_path] = {"error": str(e)}
    else:
        result[individual_rrd_path] = {"error": "RRD file not found"}

    return result

@rrd_rest.get(
    "/",
    summary="Get the data from one or multiple RRD files based on provided paths"
)
async def get_rrd(
    rrd_path: str,
    epoch_start_time: Optional[int] = None,
    epoch_end_time: Optional[int] = None
) -> Dict[str, Any]:
    """
    Fetch data from one or multiple RRD files based on the given RRD path.
    
    Args:
    rrd_path (str): A string containing the base path. It could either be a single RRD file or a port-id pattern.
    epoch_start_time (Optional[int]): Start time for the data.
    epoch_end_time (Optional[int]): End time for the data.

    Returns:
    Dict[str, Any]: Combined result from all RRD files.
    """
    
    # Check if both start and end times are specified
    if (epoch_start_time and not epoch_end_time) or (epoch_end_time and not epoch_start_time):
        raise HTTPException(
            status_code=400,
            detail="Both epoch_start_time and epoch_end_time must be specified."
        )

    results = {}

    # Check if the path is for multiple ports (e.g., includes port-id{port1,port2})
    match_multi = re.match(r"^(.*)/port-id\{(.*)\}\.rrd$", rrd_path)
    if match_multi:
        base_path = match_multi.group(1)  # Extract base path
        port_ids_str = match_multi.group(2)  # Extract port ids string
        port_ids = port_ids_str.split(",")  # Split the string into individual port ids

        # List to hold the futures
        futures = []

        # Process each port-id
        for port_id in port_ids:
            individual_rrd_path = f"{base_path}/port-id{port_id}.rrd"  # Construct full path for each port
            # Submit the task to the process pool
            futures.append(executor.submit(process_rrd_file, individual_rrd_path, epoch_start_time, epoch_end_time, port_id))

        # Wait for all processes to complete and gather results
        for future in as_completed(futures):
            result = future.result()
            results.update(result)

    else:
        # If it's a single file or other non-port RRD file
        if os.path.isfile(rrd_path):
            try:
                rr = RRD_parser(
                    rrd_file=rrd_path,
                    start_time=epoch_start_time,
                    end_time=epoch_end_time
                )
                r = rr.compile_result()
                results[rrd_path] = r
            except Exception as e:
                results[rrd_path] = {"error": str(e)}
        else:
            raise HTTPException(
                status_code=404,
                detail=f"RRD file {rrd_path} not found."
            )

    return results
