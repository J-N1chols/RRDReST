FROM python:3.10-alpine
RUN apk add g++ make rrdtool
COPY requirements.txt /opt/stacks/librenms/RRDReST/requirements.txt
WORKDIR /opt/stacks/librenms/RRDReST
RUN pip3 install -r requirements.txt
COPY . /opt/stacks/librenms/RRDReST
ENTRYPOINT ["uvicorn", "rrdrest:rrd_rest"]
CMD ["--host", "0.0.0.0", "--port", "9000" "--log-level", "debug"]
