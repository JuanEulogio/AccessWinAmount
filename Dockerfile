FROM python:3.9-slim

WORKDIR /

COPY matchdb.proto .
COPY matchdb_pb2_grpc.py .
COPY matchdb_pb2.py .
COPY server.py .
COPY client.py .
COPY inputs /inputs
COPY partitions /partitions
COPY wins /wins
COPY outputs /outputs

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip install grpcio-tools==1.66.1 grpcio==1.66.1 protobuf==5.27.2 && \
    pip install pandas
 

EXPOSE 5440

#our entry points
CMD ["python3", "-u", "/server.py"]