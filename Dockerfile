FROM python:3.10-alpine

# Install dependencies including rrdtool
RUN apk add g++ make rrdtool

# Copy requirements and install them
COPY requirements.txt /opt/stacks/librenms/RRDReST/requirements.txt
WORKDIR /opt/stacks/librenms/RRDReST
RUN pip3 install -r requirements.txt

# Copy the application code
COPY . /opt/stacks/librenms/RRDReST

# Define ENTRYPOINT for uvicorn, then use CMD for the parameters
ENTRYPOINT ["uvicorn", "rrdrest:rrd_rest"]

# Add arguments for host, port, log level, and workers
CMD ["--host", "0.0.0.0", "--port", "9000", "--log-level", "debug", "--workers", "8"]
