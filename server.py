import matchdb_pb2_grpc, grpc
from matchdb_pb2 import GetMatchResp
import pandas as pd
from concurrent import futures
import sys
import socket


class MatchCount(matchdb_pb2_grpc.MatchCountServicer):
    def __init__(self, data):
        # Read the CSV file using pandas
        self.df = data   

    #We read our cvs file, to get count of matches where
    #request.winning_team == csv winning team (* AND request.contry== csv country)
    def GetMatchCount(self, request, context):
        #fetch by filter options, dont filter any that have "", ignore it
        count= 0
        if(request.winning_team == "" and request.country== ""):
            count= df.shape[0]
        elif request.country== "":
            count= len(df[
                df["winning_team"]==request.winning_team
            ])
        elif request.winning_team== "":
            count= len(df[
                df["country"]==request.country
            ])
        else:
            count = len(df[
                ( df["winning_team"]==request.winning_team) & 
                ( df["country"]== request.country )
            ])
        
        return (GetMatchResp(num_matches= count))




def determineDataPart():
    container_ip = socket.gethostbyname(socket.gethostname())
    server1_ip = socket.gethostbyname("wins-server-1")
    server2_ip = socket.gethostbyname("wins-server-2")

    if container_ip == server1_ip:
        return "partitions/part_0.csv"
    elif container_ip == server2_ip:
        return "partitions/part_1.csv"

# Function to serve the gRPC server
def server(data, port):
    print("starting server " + str(port))
    #print(data)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=[("grpc.so_reuseport", 0)])
    #pass csv_dataFrame
    matchdb_pb2_grpc.add_MatchCountServicer_to_server(MatchCount(data), server)
    server.add_insecure_port("0.0.0.0:" + str(port))
    server.start()
    server.wait_for_termination()

#read our terminal commands to see ARGS/parameter
if __name__ == '__main__':
    # Check for correct command line arguments
    if len(sys.argv) != 3:
        csv_file = determineDataPart()
        port = 5440
    else:
        # Extract arguments
        csv_file = sys.argv[1]
        port = int(sys.argv[2])    

    #convert csv file to pandas dataFrame
    df = pd.read_csv(csv_file)
    # Start the gRPC server
    server(df, port)