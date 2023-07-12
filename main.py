import json
import socket
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from queue import Queue
from threading import Lock, Thread
from time import sleep

import zmq
from sympy import simplify

from DB_Utils import DB_Utils



######### CONSTANTS ##########
TASK_QUEUE = Queue(maxsize=1000000)
USER_GRAMMAR_CONTAINER = dict()
UID_TOKEN_CONTAINER = dict()
UID_GRAMMAR_CONTAINER = dict()
# DICT_LOCK = Lock()
##############################


'''
Starting Here, Receiver and Worker is written.
'''

def Receiver(UGC_LOCK):

    context = zmq.Context()

    # This Socket is for SUBSCRIBING and UN-SUBSCRIBING from message broker present in zeroMQ 
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect("tcp://127.0.0.1:6667")
    
    # This Socket is for Sending Data to Server for Getting Token Data so that Server may send data to Client
    req_socket = context.socket(zmq.REQ)
    req_socket.connect("tcp://127.0.0.1:6666")

    try:

        req_socket.send_string(f"SUBSCRIBE {-1}")
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        response = req_socket.recv_string()
        print(response)

    except zmq.error.ZMQError as zmqError:

        print(zmqError)
        print("~~~")
        pass

    while (True):

        Data = sub_socket.recv()
        snapShot_Data = json.loads(Data.decode())

        # print("from Receiver SIZE: ",TASK_QUEUE.qsize(), "ID ->", id(TASK_QUEUE))
        UGC_LOCK.acquire()
        for uid in USER_GRAMMAR_CONTAINER.keys():
            TASK_QUEUE.put((snapShot_Data, uid))
        UGC_LOCK.release()
        sleep(0.001)
            
def Worker(DLOCK):

    PoolInstance = ThreadPoolExecutor(max_workers=32)

    while (True):

        if (not TASK_QUEUE.empty()):
            Task = TASK_QUEUE.get()
            for i in USER_GRAMMAR_CONTAINER[Task[1]].keys():
                Parse_Obj = Grammar_Parser()
                PoolInstance.submit(Parse_Obj.Value_Checking, Task[0], Task[1], i, DLOCK)
                
                pass

def handle_client(client_socket, DLOCK, UGC_LOCK):
    
    client_socket.settimeout(10)
    
    while True:
        try:
            grammar = client_socket.recv(1024).decode('utf-8')
            # print("DECODED GRAMMAR: ", grammar)
            grammar = json.loads(grammar)
            # print("DECODED GRAMMAR: ", type(grammar))

            UGC_LOCK.acquire()
            UID = grammar["UID"]
            print("**UID**: ", UID)
            if UID not in USER_GRAMMAR_CONTAINER.keys():
                USER_GRAMMAR_CONTAINER[UID] = dict()
            for gram_id in grammar.keys():
                if gram_id == "UID": continue
                USER_GRAMMAR_CONTAINER[UID][gram_id] = dict()
                USER_GRAMMAR_CONTAINER[UID][gram_id] = grammar[gram_id]

            UGC_LOCK.release()
            
            update_container()
            
        except socket.timeout:
            print("!**!")
        
        print(".....sending data.....")
        DLOCK.acquire()
        try:
            for gram_id in UID_TOKEN_CONTAINER[UID].keys():
                temp = 0
                for token in UID_TOKEN_CONTAINER[UID][gram_id][UID_GRAMMAR_CONTAINER[UID][gram_id]:]:
                    client_socket.send((json.dumps({gram_id:token})+"\n").encode('utf-8'))
                    # print("#|#|#|#|# getsizeof: ", sys.getsizeof((json.dumps({gram_id:token})+"\n")))
                    temp += 1
                UID_GRAMMAR_CONTAINER[UID][gram_id] += temp
        except KeyError as KE:
            print("K-E")
            pass
        DLOCK.release()

def client_connect_server(DLOCK, UGC_LOCK):
    host = '127.0.0.1'
    port = 5001

    s = socket.socket()
    s.bind((host, port))
    s.listen()
    print(f"Server listening on {host}:{port}")

    # UID = 1
    while True:
        client_socket, addr = s.accept()
        t = Thread(target=handle_client, args=(client_socket, DLOCK, UGC_LOCK))
        t.start()
        # UID += 1
        sleep(5)
        print(USER_GRAMMAR_CONTAINER)

def update_container():
    
    for UID in USER_GRAMMAR_CONTAINER.keys():

        if UID not in UID_TOKEN_CONTAINER.keys():
            UID_TOKEN_CONTAINER[UID] = dict()
        for Grammar in USER_GRAMMAR_CONTAINER[UID].keys():
            if Grammar not in UID_TOKEN_CONTAINER[UID].keys():
                UID_TOKEN_CONTAINER[UID][Grammar] = list()
        

        if UID not in UID_GRAMMAR_CONTAINER.keys():
            UID_GRAMMAR_CONTAINER[UID] = dict()
        for Grammar in USER_GRAMMAR_CONTAINER[UID].keys():
            if Grammar not in UID_GRAMMAR_CONTAINER[UID].keys():
                UID_GRAMMAR_CONTAINER[UID][Grammar] = 0

    print("/*\ UID_TOKEN_CONTAINER: ", UID_TOKEN_CONTAINER)
    print("/*\ UID_GRAMMAR_CONTAINER: ", UID_GRAMMAR_CONTAINER)


'''
This Class is Grammar Parser
'''

class Grammar_Parser:        

##### UPDATE DIFFERENT FUNCTIONS WHICH CAN EXTRACT SPECIFIC DETAILS FROM Single SnapShot Data ########################################
    def getToken(self) -> int: return self.SnapShot_Data["Token"]

    def getOI(self) -> int: return self.SnapShot_Data["OpenInterest"]
#######################################################################################################################################
    

###### CONSTRUCTOR ######################################################################################################################
    def __init__(self) -> None:
        pass
##########################################################################################################################################


###### FUNCTION USED TO GET VALUES OF ANY VARIABLE VALUES PRESENT IN GRAMMAR ##############################################################
    def variable_value_assigner(self, JSON) -> None:

        # UPDATE FUNCTION VALUE DICTIONARY WITH FUNCTION NAME USED TO EXTRACT DATA FROM SnapShot Data
        self.FUNCTION_VALUE = {
                'getToken': self.getToken(),
                'getOI': self.getOI()
        }
        
            # UPDATE DATABASE FUNCTION VALUE DICTIONARY WITH FUNCTION NAME USED TO EXTRACT DATA FROM DB_UTILS CLASS
        self.DB_FUNCTION = {
                'getFutureForSecurity': self.DB_Obj.getFutureForSecurity(self.TOKEN),
                'getNextStrike': self.DB_Obj.getNextStrike(self.TOKEN),
                'getInstrumentType': self.DB_Obj.getInstrumentType(self.TOKEN),
                'isCall': self.DB_Obj.isCall(self.TOKEN),
                'isPut': self.DB_Obj.isPut(self.TOKEN)
        }
        
       
        for i in JSON.keys() :

            try:
                if JSON[i] in self.FUNCTION_VALUE.keys():
                    JSON[i] = self.FUNCTION_VALUE[JSON[i]]
                    continue
            except KeyError as KE:
                pass
            
            try:
                if JSON[i] in self.DB_FUNCTION.keys():
                    JSON[i] = self.DB_FUNCTION[JSON[i]]
                    continue
            except KeyError as KE:
                pass
############################################################################################################################################


###### FUNCTION TO CHECK ALL THE CONDITIONS PRESENT IN GRAMMAR #############################################################################
    def condition(self, next:str = "Primary") -> tuple :
        
        # print("Values: ",self.GRAMMER[next]["values"])
        self.variable_value_assigner(self.GRAMMER[next]["values"])

        if next == "Primary":

            result = simplify(self.GRAMMER[next]["expression"]).subs(self.GRAMMER[next]["values"])
            next = self.GRAMMER[next]["next"]

            return (result, next)
        
        else:
            final_result = True
            while (next != "None"):

                result = simplify(self.GRAMMER[next]["expression"]).subs(self.GRAMMER[next]["values"])
                next = self.GRAMMER[next]["next"]
                print("Condition Result is: ", result)
                print("Condition Next is: ", next)

                if result:
                    continue
                else:
                    final_result = False
                    break
            
            return (final_result, self.TOKEN)
###############################################################################################################################################
    

##### FUNCTION WHICH IS CALLED FROM THE WORKER FUNCTION ###################################################################
    def Value_Checking(self, SnapShot_Data, UID, GrammarID, D_LOCK) -> None:
        
        self.UID = UID
        self.SnapShot_Data = SnapShot_Data
        self.GRAMMER = deepcopy(USER_GRAMMAR_CONTAINER[self.UID][GrammarID])
        self.DB_Obj = DB_Utils()
        self.TOKEN = self.SnapShot_Data['Token']
        print("TOKEN: ", self.TOKEN)
        print("Values: ",self.GRAMMER["Primary"]["values"])
        

        result, next = self.condition()

        if result:
            response, token = self.condition(next)
            print("RESULT and NEXT is completed once......", response, " - ", token)
            if response :
                print("UID: ", UID," GrammarID: ", GrammarID, " TOKEN: ", token)

                D_LOCK.acquire()
                if token not in UID_TOKEN_CONTAINER[UID][GrammarID]: UID_TOKEN_CONTAINER[UID][GrammarID].append(token)
                D_LOCK.release()
###########################################################################################################################



if __name__ == "__main__":
    
    print("1.")

    UID_TOKEN_LOCK = Lock()
    USER_GRAMMAR_LOCK = Lock()
    

    P1 = Thread(target = Receiver, args=(USER_GRAMMAR_LOCK, ))
    P2 = Thread(target = Worker, args=(UID_TOKEN_LOCK,))
    P3 = Thread(target=client_connect_server, args=(UID_TOKEN_LOCK, USER_GRAMMAR_LOCK))

    P3.start()

    P1.start()
    P2.start()
    
    sleep(20)
    UID_TOKEN_LOCK.acquire()
    print(UID_TOKEN_CONTAINER, file=open("UID_TOKEN_CONTAINER.txt", "w"))
    UID_TOKEN_LOCK.release()