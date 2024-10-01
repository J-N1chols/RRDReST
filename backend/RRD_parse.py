import subprocess
import xmltodict
import json
import re
from collections import defaultdict
from itertools import chain
import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

class RRD_parser:

    def __init__(self, rrd_file=None, start_time=None, end_time=None):
        self.rrd_file = rrd_file
        self.port_id = self.extract_port_id(rrd_file)  # Extract port-id if present
        self.ds = None
        self.step = None
        self.time_format = "%Y-%m-%d %H:%M:%S"
        self.check_dependc()
        self.start_time = start_time
        self.end_time = end_time

    def extract_port_id(self, rrd_file):
        """ Extracts the port-id from the filename, if present. Assumes the pattern 'port-idXX' or returns None if not applicable. """
        match = re.search(r'port-id(\d+)', rrd_file)
        return f"port-id{match.group(1)}" if match else None

    def check_dependc(self):
        """ Check if RRDtool is installed """
        result = subprocess.check_output(
            "rrdtool --version",
            shell=True
        ).decode('utf-8')
        if "RRDtool 1." not in result:
            raise Exception("RRDtool version not found, check rrdtool installed")

    def get_data_source(self):
        """ Gets data sources from RRD file using rrdtool info """
        STEP_VAL = None
        DS_VALS = []

        result = subprocess.check_output(
            f"rrdtool info {self.rrd_file}",
            shell=True
        ).decode('utf-8')

        temp_arr = result.split("\n")

        for line in temp_arr:
            if " = " in line:
                raw_key = line.split(" = ")[0]
                raw_val = line.split(" = ")[1]

            if raw_key == "step":
                STEP_VAL = raw_val

            if ("ds[" in raw_key) and ("]." in raw_key):
                match_obj = re.match(r'^ds\[(.*)\]', raw_key)
                if match_obj:
                    ds_val = match_obj.group(1)
                    if ds_val not in DS_VALS:
                        DS_VALS.append(ds_val)
        self.step = STEP_VAL
        self.ds = DS_VALS

    def get_rrd_json(self, ds):
        """ Get RRD data in JSON format using rrdtool xport """
        rrd_xport_command = f"rrdtool xport --step {self.step} DEF:data={self.rrd_file}:{ds}:AVERAGE XPORT:data:{ds} --showtime"
        if self.start_time:
            rrd_xport_command += f" --start {self.start_time} --end {self.end_time}"
        
        result = subprocess.check_output(
            rrd_xport_command,
            shell=True
        ).decode('utf-8')
        json_result = json.dumps(xmltodict.parse(result), indent=4)

        # Replace "v" with the actual data source name
        replace_val = f"\"{ds.lower()}\": "
        temp_result_one = re.sub("\"v\": ", replace_val, json_result)
        return json.loads(temp_result_one)

    def cleanup_payload(self, payload):
        """ Clean up and transform the response payload """
        for count, temp_obj in enumerate(payload["data"]):
            epoch_time = temp_obj["t"]
            utc_time = datetime.datetime.fromtimestamp(
                int(epoch_time)
            ).strftime(self.time_format)
            payload["data"][count]["t"] = utc_time

            for key in payload["data"][count]:
                if isinstance(payload["data"][count][key], str) and ("e+" in payload["data"][count][key] or "e-" in payload["data"][count][key]):
                    payload["data"][count][key] = float(payload["data"][count][key])

        pl = json.dumps(payload)
        pl = re.sub(r'\"(\d+)\"', r'\1', pl)
        pl = re.sub(r'\"(\d+\.\d+)\"', r'\1', pl)
        pl = re.sub(r'\"NaN\"', "null", pl)
        pl = re.sub(r'\"t\"', r'"time"', pl)

        return json.loads(pl)

    def compile_result(self):
        """ Compile the final result from the RRD file """
        self.get_data_source()
        DS_VALUES = self.ds
        master_result = {
            "meta": {
                "start": "",
                "step": "",
                "end": "",
                "rows": "",
                "data_sources": []
            },
            "data": [],
        }

        collector = defaultdict(dict)

        for d in DS_VALUES:
            r = self.get_rrd_json(ds=d)
            master_result["meta"]["start"] = datetime.datetime.fromtimestamp(
                int(r["xport"]["meta"]["start"])
            ).strftime(self.time_format)
            master_result["meta"]["step"] = r["xport"]["meta"]["step"]
            master_result["meta"]["end"] = datetime.datetime.fromtimestamp(
                int(r["xport"]["meta"]["end"])
            ).strftime(self.time_format)
            master_result["meta"]["rows"] = 0
            master_result["meta"]["data_sources"].append(
                r["xport"]["meta"]["legend"]["entry"]
            )

            for collectible in chain(
                master_result["data"], r["xport"]["data"]["row"]
            ):
                collector[collectible["t"]].update(collectible.items())

        combined_list = list(collector.values())
        master_result["data"] = combined_list
        master_result["meta"]["rows"] = len(combined_list)
        final_result = self.cleanup_payload(master_result)

        # Add port-id to data entries if applicable
        if self.port_id:
            for entry in final_result["data"]:
                entry["port-id"] = self.port_id

        return final_result

    @staticmethod
    def process_port(rrd_file, start_time=None, end_time=None):
        """ Static method to process an individual port in parallel """
        parser = RRD_parser(rrd_file, start_time, end_time)
        return parser.compile_result()

# Use ProcessPoolExecutor for parallel processing
def process_multiple_ports(rrd_files, start_time=None, end_time=None, max_workers=4):
    """ Process multiple RRD files concurrently using multiple processes """
    results = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_rrd = {executor.submit(RRD_parser.process_port, rrd_file, start_time, end_time): rrd_file for rrd_file in rrd_files}

        for future in as_completed(future_to_rrd):
            rrd_file = future_to_rrd[future]
            try:
                result = future.result()
                results[rrd_file] = result
            except Exception as exc:
                results[rrd_file] = {"error": str(exc)}

    return results
