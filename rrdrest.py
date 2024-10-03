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
    version="0.5",  # Updated version
)

# Define the process pool
executor = ProcessPoolExecutor(max_workers=4)  # Adjust max_workers for x=CPU cores.

def process_rrd_file(individual_rrd_path: str, epoch_start_time: Optional[int], epoch_end_time: Optional[int], port_id: Optional[str] = None, ent_physical_index: Optional[str] = None) -> Dict[str, Any]:
    """Function to process an individual RRD file and add port_id or entPhysicalIndex."""
    result = {}
    if os.path.isfile(individual_rrd_path):
        try:
            rr = RRD_parser(
                rrd_file=individual_rrd_path,
                start_time=epoch_start_time,
                end_time=epoch_end_time
            )
            r = rr.compile_result()

            # Add port_id or entPhysicalIndex to the data
            if "data" in r:
                for entry in r["data"]:
                    if port_id:
                        entry["port_id"] = port_id  # Add port_id if available
                    if ent_physical_index:
                        entry["entPhysicalIndex"] = ent_physical_index  # Add entPhysicalIndex if available

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
    rrd_path (str): A string containing the base path. It could either be a single RRD file or a pattern.
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

    # Check for multiple ports (e.g., includes port-id{port1,port2})
    match_multi_port = re.match(r"^(.*)/port-id\{(.*)\}\.rrd$", rrd_path)
    
    # Check for multiple sensors (e.g., includes sensor indices in {})
    match_sensor_group = re.match(r"^(.*)/sensor-(\w+)-cisco-entity-sensor-\{(.*)\}\.rrd$", rrd_path)

    # Check for single sensor files or port
    match_single_sensor = re.match(r"^(.*)/sensor-(\w+)-cisco-entity-sensor-(\d+)\.rrd$", rrd_path)
    match_single_port = re.match(r"^(.*)/port-id(\d+)\.rrd$", rrd_path)

    # List to hold the futures
    futures = []

    if match_multi_port:
        base_path = match_multi_port.group(1)  # Extract base path
        port_ids_str = match_multi_port.group(2)  # Extract port ids string
        port_ids = port_ids_str.split(",")  # Split the string into individual port ids

        # Process each port-id
        for port_id in port_ids:
            individual_rrd_path = f"{base_path}/port-id{port_id}.rrd"  # Construct full path for each port
            # Submit the task to the process pool
            futures.append(executor.submit(process_rrd_file, individual_rrd_path, epoch_start_time, epoch_end_time, port_id=port_id))

    elif match_sensor_group:
        base_path = match_sensor_group.group(1)  # Extract base path
        sensor_type = match_sensor_group.group(2)  # Extract sensor type (e.g., dbm, current, etc.)
        sensor_indices_str = match_sensor_group.group(3)  # Extract sensor indices string
        sensor_indices = sensor_indices_str.split(",")  # Split the string into individual sensor indices

        # Process each sensor index separately
        for sensor_index in sensor_indices:
            individual_rrd_path = f"{base_path}/sensor-{sensor_type}-cisco-entity-sensor-{sensor_index}.rrd"  # Construct full path for each sensor
            # Submit the task to the process pool
            futures.append(executor.submit(process_rrd_file, individual_rrd_path, epoch_start_time, epoch_end_time, ent_physical_index=sensor_index))

    elif match_single_sensor:
        base_path = match_single_sensor.group(1)  # Extract base path
        sensor_type = match_single_sensor.group(2)  # Extract sensor type (e.g., dbm, current, etc.)
        sensor_index = match_single_sensor.group(3)  # Extract the sensor index

        individual_rrd_path = f"{base_path}/sensor-{sensor_type}-cisco-entity-sensor-{sensor_index}.rrd"
        futures.append(executor.submit(process_rrd_file, individual_rrd_path, epoch_start_time, epoch_end_time, ent_physical_index=sensor_index))

    elif match_single_port:
        base_path = match_single_port.group(1)  # Extract base path
        port_id = match_single_port.group(2)  # Extract the port id

        individual_rrd_path = f"{base_path}/port-id{port_id}.rrd"
        futures.append(executor.submit(process_rrd_file, individual_rrd_path, epoch_start_time, epoch_end_time, port_id=port_id))

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

    # Wait for all processes to complete and gather results
    for future in as_completed(futures):
        result = future.result()
        results.update(result)

    return results
