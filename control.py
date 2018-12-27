import sys, socket
from thread import *
import threading
import time
import string
import random
from collections import namedtuple

def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['reverse_mapping'] = reverse
    return type('Enum', (), enums)

rv = ("127.0.0.1", 10000)

client = namedtuple("client", "id ip port group state nick")
state = enum("connected","joining","leader","member")


groups = {}
groups_lock = threading.Lock()

msgs = {}

def randomStr(size=4, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for x in range(size))
def genGroupId(groups):
    gid = randomStr()
    while(groups.get(gid) != None):
        gid = randomStr()
    return gid
"""

RPCs

if status == connected
1. new(){ returns id of new group after creating it }
2. join(group id){ returns ip, port of group leader }
else
1. newleader(gid,ip,port)

"""


def t_send_message(mid,ip_address, port, msg, wait = False):
    global msgs
    rmsg = msg
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        c.connect((ip_address, port))
    except Exception, e:
        print("Connection Refused", "The Address You Are Trying To Reach Is Currently Unavailable")
        if(wait):
            msgs[mid][0] = 0
            msgs[mid][2].release()
        return

    try:
        c.send(rmsg)
        if(wait):
            data = c.recv(8192)
            msgs[mid][1] = data
            print("adsfasdfasdfasdf      "+data+"      +sdfasdfasdf")
    except Exception, e:
        print("Connection Refused", "The Message Cannot Be Sent. End-Point Not Connected !!")
        if(wait):
            msgs[mid][0] = 0

    c.close()
    if(wait):
        msgs[mid][2].release()

def send_message(x,msg, wait = False):
    global msgs
    d = True
    if(wait):
        mid = randomStr(20)
        while(msgs.get(mid) != None):
            mid = randomStr(20)
        msgs[mid] = (1,False,threading.Lock())
        msgs[mid][2].acquire()
        start_new_thread(t_send_message,(mid,x[0],x[1],msg))
        msgs[mid][2].acquire()
        msgs[mid][2].release()
        d = msgs[mid][1]
        if(msgs[mid][0] == 0):
            d = False
        msgs.remove(mid)
    else:
        start_new_thread(t_send_message,(-1,x[0],x[1],msg))
    return d

def s_send_message(x ,msg,wait=False):
    ip_address, port = x[0],x[1]
    rmsg = msg
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data = True
    try:
        c.connect((ip_address, port))
    except Exception, e:
        print("Connection Refused", "The Address You Are Trying To Reach Is Currently Unavailable")
        return False

    try:
        c.send(rmsg)
        if(wait):
            data = c.recv(8192)
    except Exception, e:
        print("Connection Refused", "The Message Cannot Be Sent. End-Point Not Connected !!")
        return False
    
    c.close()
    return data

def t_recieve(conn,addr):
    global groups, groups_lock
    data = conn.recv(8192)
    print("\n("+addr[0]+":"+str(addr[1])+") > "+data+"\n")
    d = data.split(",")
    if(d[0] == "ping" and len(d) == 1):
        conn.send("ack")
    elif(d[0] == "new" and len(d) == 2 and d[1].isdigit()):
        gid = genGroupId(groups)
        d[1] = int(d[1])
        if(s_send_message((addr[0] , d[1]), "ping", True) == "ack"):
            groups[gid] = (addr[0],d[1])
            conn.send("ack,"+gid)
        else:
            conn.send("nak,server not reachable")
    elif(d[0] == "join" and len(d) == 3 and d[1].isdigit()):
        gid = d[2]
        print("asf")
        r = groups.get(gid)
        
        if(r != None):
            r = "ack,"+r[0]+","+str(r[1])
        else:
            r = "nak,None"
        print r
        conn.send(r)
    elif(d[0] == "newleader" and len(d) == 4 and d[3].isdigit()):
        groups_lock.acquire()
        l = groups.get(d[1])
        groups_lock.release()

        if(l == None):
            l = "nak,invalid group"
        elif(s_send_message(l, "newleader,"+d[2]+","+d[3], True) == "ack"):
            groups_lock.acquire()
            groups[d[1]] = (d[2],int(d[3]))
            groups_lock.release()
            l = "ack,success"
        else:
            l = "nak,unauthorized request"
        conn.send(l)
    else:
        conn.send("nak,bad request")
            
    conn.close()

def server_socket(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', port))
        s.listen(5)
    except socket.error, e:
        print("Socket Error !!",
            "Unable To Setup Local Socket. Port In Use")
        return
 
    while 1:
        conn, addr = s.accept()
        incoming_ip = str(addr[0])
        
        start_new_thread(t_recieve, (conn,addr))
    s.close()

start_new_thread(server_socket, (10000,))

q = 1
while(q):
    text = raw_input("> ")
    if(text == "quit"):
        q = 0
    elif(text == "log"):
        print(groups)