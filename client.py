import grpc
import sys
from matchdb_pb2 import GetMatchReq
from matchdb_pb2_grpc import MatchCountStub
import pandas as pd
from collections import OrderedDict

#cache object
class LRUCache:
    def __init__(self):
        self.cache = OrderedDict()

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        else:
            self.cache[key] = value
            if len(self.cache) > 10:
                self.cache.popitem(last=False)
    
    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        
        return None
        


def simple_hash(country):
    out = 0
    for c in country:
        out += (out << 2) - out + ord(c)
    return out%2


#check if all args were filled in
if len(sys.argv) != 4:
    print("Wrong input. Format: python client.py <SERVER_0> <SERVER_1> <INPUT_FILE>")
    sys.exit(1)



server_0, server_1, input_file = sys.argv[1], sys.argv[2], sys.argv[3]

channel_0 = grpc.insecure_channel(server_0)
channel_1 = grpc.insecure_channel(server_1)
stub_0 = MatchCountStub(channel_0)
stub_1 = MatchCountStub(channel_1)
df = pd.read_csv(input_file)


cache= LRUCache() 

#call method, determins what server(channel) to contact
#iterates the inputs, does the process for whatever that input[i] is
#we use stub inside the iterations then, and resp= count
for _,row in df.iterrows():
    count=0
    
    winningTeam= row.get("winning_team")
    if( pd.isnull( row.get("winning_team") ) ):
        winningTeam= ""
    
    country= row.get("country")
    if( pd.isnull( row.get("country") ) ):
        country= ""


    # print(winningTeam + "," + country)

    #checks if same filters are in cache
    key = "{},{}".format(winningTeam, country)
    count= cache.get(key)
    if(count != None):
        print( str(count) + "*" )
        continue

    if( country == "" ):
        resp0= stub_0.GetMatchCount(GetMatchReq( country= "",  winning_team= winningTeam))
        resp1= stub_1.GetMatchCount(GetMatchReq( country= "",  winning_team= winningTeam))
        count= resp0.num_matches + resp1.num_matches

    elif(simple_hash( country) == 0):
        resp= stub_0.GetMatchCount(GetMatchReq( country= country,  winning_team= winningTeam ))
        count= resp.num_matches

    else:
        resp= stub_1.GetMatchCount(GetMatchReq( country= country,  winning_team= winningTeam ))
        count= resp.num_matches
    
    #queue in cache
    cache.put( key, count)
    print(count)