import sys ,getopt, os, time
import threading
import zmq
from random import sample
from multiprocessing import Manager, Process
from threading import Thread

def response(nodeID, shared_dict, numproc):
    print("RESPONDER STARTS: {}".format(nodeID))
    context = zmq.Context()
    subs_socket = context.socket(zmq.SUB)
    subs_socket.subscribe("TERMINATE")
    subs_socket.subscribe("LEADER")
    poller_sc =zmq.Poller()
    poller_sc.register(subs_socket, zmq.POLLIN)
    publisher_sc = context.socket(zmq.PUB)
    publisher_sc.bind("tcp://127.0.0.1:{}".format(5500+ nodeID + numproc)) #check here again
    is_on_terminate = False
    for i in range(numproc):
        subs_socket.connect("tcp://127.0.0.1:{}".format(5500+ i))
    while not is_on_terminate:
        topic = subs_socket.recv_string()
        message = subs_socket.recv_json()
        for key, senderNodeId in message.items():
            if topic == 'TERMINATE':
                with threading.Lock():
                    shared_dict["Terminate"] = True
            elif topic == "LEADER":
                if senderNodeId < nodeID:
                    multicast = 1
                else:
                    multicast = 2
                with threading.Lock():
                    shared_dict["Multicast"] = multicast
                if senderNodeId < nodeID:
                    mess_to_send = {'RESPONSE': "{} {}".format(nodeID, senderNodeId)}   
                    time.sleep(0.05) 
                    publisher_sc.send_string("RESPONSE", flags=zmq.SNDMORE)
                    publisher_sc.send_json(mess_to_send)
                    print("RESPONDER RESPONDS {} {}".format(nodeID,senderNodeId))
        with threading.Lock():
            is_on_terminate = shared_dict["Terminate"]

def leader(nodeID, starter = False, numproc= 0):
    print("PROCESS STARTS {} {} {}".format(os.getpid(),nodeID,starter))
    manager = Manager()
    shared_dict = manager.dict()
    shared_dict["Terminate"] = False
    shared_dict["Multicast"] = 0
    listener_thread = Thread(target= response, args=(nodeID, shared_dict, numproc))
    listener_thread.start()
    context = zmq.Context()
    pub_sc = context.socket(zmq.PUB)
    pub_sc.bind("tcp://127.0.0.1:{}".format(5500+ nodeID))
    onmulticast = 0
    if  not starter:
        with threading.Lock():
            onterminate = shared_dict["Terminate"]
        while True and not onterminate:
            with threading.Lock():
                onmulticast = shared_dict["Multicast"]
                onterminate = shared_dict["Terminate"]
            if onmulticast>0: break

    if onmulticast == 1 or starter: 
        print("PROCESS MULTICASTS LEADER MSG: {}".format(nodeID))
        message = {"LEADER": nodeID}
        time.sleep(1)
        pub_sc.send_string("LEADER", flags=zmq.SNDMORE)
        pub_sc.send_json(message)
        subs_sc = context.socket(zmq.SUB)
        subs_sc.subscribe("RESPONSE")
        poller_sc =zmq.Poller()
        poller_sc.register(subs_sc, zmq.POLLIN)
        for i in range(numproc):
            if i != nodeID : subs_sc.connect("tcp://127.0.0.1:{}".format(5500+ i + numproc))
        check = False
        with threading.Lock():
            onterminate = shared_dict["Terminate"]
        timeout = time.time() + (1)
        while timeout > time.time():
            polls = dict(poller_sc.poll(timeout=100))
            if subs_sc in polls:
                topic = subs_sc.recv_string()
                rec_mess = subs_sc.recv_json()
                for key, value in rec_mess.items():
                    to = value.split(" ")[1]
                    if int(to) == nodeID:
                        check = True 
                with threading.Lock():
                    onterminate = shared_dict["Terminate"]
        if not check and not onterminate:
            term_mess = {'TERMINATE': nodeID}
            print("PROCESS BROADCASTS TERMINATE MSG: {}".format(nodeID))
            pub_sc.send_string("TERMINATE", zmq.SNDMORE)
            pub_sc.send_json(term_mess)
            with threading.Lock():
                shared_dict["Terminate"] = True
    listener_thread.join()


def main(argv):
    numProc = 0
    numAlive = 0
    numStarter = 0
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["numproc=","numalive=", "numstarter="])
    except getopt.GetoptError:
        print('usage: bully.py --numproc <numproc> --numstarter <numstarter> --numalive <numalive>')
        sys.exit(2)
    if len(opts) < 3:
        print('usage: bully.py --numproc <numproc> --numstarter <numstarter> --numalive <numalive>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '--numproc':
            numProc = int(arg)
        elif opt == '--numalive':
            numAlive = int(arg)
        elif opt == '--numstarter':
            numStarter = int(arg)
        elif opt == '-h':
            print('usage: bully.py --numproc <numproc> --numstarter <numstarter> --numalive <numalive>')
    if (numProc < numAlive) or (numAlive < numStarter):
        print('usage: bully.py --numproc <numproc> --numstarter <numstarter> --numalive <numalive>')
        sys.exit(2)
    
    alive_nodes = sample([i for i in range(numProc)], numAlive)
    starter_nodes = sample(alive_nodes, numStarter)
    print('Alives: {}\nStarters: {}'.format(alive_nodes, starter_nodes))
    leader_processes = []
    for node in alive_nodes:
        if node in starter_nodes: 
            leader_processes.append(Process(target=leader, args=(node, True, numProc)))
        else: 
            leader_processes.append(Process(target=leader, args=(node, False, numProc)))

    for proccess in leader_processes:
        proccess.start()

    for proccess in leader_processes:
        proccess.join()
        

if __name__ == "__main__":
    main(sys.argv[1:])