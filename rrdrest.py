from fastapi import FastAPI, HTTPException
from backend.RRD_parse import RRD_parser
from typing import Optional, List, Dict, Any
import os
import re

rrd_rest = FastAPI(
    title="RRDReST",
    description="Makes RRD files API-able",
    version="0.2",
)


@rrd_rest.get(
    "/",
    summary="Get the data from multiple RRD files based on provided port-ids in the path"
)
async def get_rrd(
    rrd_path: str,
    epoch_start_time: Optional[int] = None,
    epoch_end_time: Optional[int] = None
) -> Dict[str, Any]:
    """
    Fetch data from multiple RRD files based on given port IDs within the rrd_path.
    
    Args:
    rrd_path (str): A string containing the base path and port-ids within curly braces.
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

    # Extract base path and port-ids using regular expression
    match = re.match(r"^(.*)/port-id\{(.*)\}\.rrd$", rrd_path)
    if not match:
        raise HTTPException(
            status_code=400,
            detail="Invalid rrd_path format. Expected format: /base/path/port-id{port1,port2}.rrd"
        )

    base_path = match.group(1)  # Extract base path
    port_ids_str = match.group(2)  # Extract port ids string
    port_ids = port_ids_str.split(",")  # Split the string into individual port ids

    results = {}
    for port_id in port_ids:
        individual_rrd_path = f"{base_path}/port-id{port_id}.rrd"  # Construct the full path

        if os.path.isfile(individual_rrd_path):
            try:
                # Pass port_id to the RRD_parser
                rr = RRD_parser(
                    rrd_file=individual_rrd_path,
                    start_time=epoch_start_time,
                    end_time=epoch_end_time
                )
                r = rr.compile_result()
                
                # Insert the port-id into the result
                for entry in r["data"]:
                    entry["port-id"] = f"port-id{port_id}"
                
                results[individual_rrd_path] = r
            except Exception as e:
                results[individual_rrd_path] = {"error": str(e)}
        else:
            results[individual_rrd_path] = {"error": "RRD file not found"}

    return results
