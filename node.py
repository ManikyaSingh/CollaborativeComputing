import sys, socket
from thread import *
import threading
import time
import string
import random
from collections import deque

def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['reverse_mapping'] = reverse
    return type('Enum', (), enums)

# print_lock = threading.Lock()
# print_lock.acquire()
# print_lock.release()

rv = ("127.0.0.1", 10000)

state = -1
view = {}
leader = None
myid = None
mpport = None
mynick = None
iclock = {}
newm = None
wQ = deque([])
dQ = {}
dQn = 0
msgs = {}

def randomStr(size=10, chars=string.ascii_letters + string.digits):
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
        print("\nConnection Refused", "The Address You Are Trying To Reach Is Currently Unavailable"+"\n > ")
        if(wait):
            msgs[mid][0] = 0
            msgs[mid][2].release()
        return

    try:
        c.send(rmsg)
        if(wait):
            data = c.recv(8192)
            msgs[mid][1] = data
    except Exception, e:
        print("\nConnection Refused", "The Message Cannot Be Sent. End-Point Not Connected !!\n > ")
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
        print("\nConnection Refused", "The Address You Are Trying To Reach Is Currently Unavailable"+"\n > ")
        return False

    try:
        c.send(rmsg)
        if(wait):
            data = c.recv(8192)
    except Exception, e:
        print("\nConnection Refused", "The Message Cannot Be Sent. End-Point Not Connected !!"+"\n > ")
        return False
    
    c.close()
    return data

def sendtoall(msg):
    global view
    for i in view:
        if(i != myid):
            send_message(view[i],msg)
    return True

def sendtoall_m(msg):
    global view
    for i in view:
        send_message(view[i],msg)
    return True

def getorder(mcs,data,s,mclock):
    global myid,dQ,dQn,view,leader
    d = data.split(",")
    if(myid == leader):
        view[d[1]][4] = mclock[d[1]]
        msg = "order,"+myid+","+mcs+","+str(dQn)
        dQn += 1
        d = data.split(",")
        print("\n"+view[d[1]][2]+" > "+d[3]+"\n > ")
        sendtoall(msg)
    else:
        if(dQ.get(mcs) == None):
            dQ[mcs] = [-1,data]
        else:
            dQ[mcs][1] = data
            if(dQn == dQ[mcs][0]):
                d = dQ[mcs][1].split(",")
                print("\n"+view[d[1]][2]+" > "+d[3]+"\n > ")
                dQn += 1
            else:
                dQ[mcs][1] = data
            dqc = True
            while(dqc):
                dqc = False
                for i in dQ:
                    if(dQ[i][0] == dQn):
                        d = dQ[i][1].split(",")
                        print("\n"+view[d[1]][2]+" > "+d[3]+"\n > ")
                        dQn += 1
                        dqc = True
    return True

def sendorder(mcs,m):
    msg = "order,"+mcs+","+m
    for i in view:
        send_message(view[i],msg)
    return True

def deliver():
    global dQ
    m = 0
    

def cmpClock(c1,c2,s):
    r = True
    if(s != False):
        for i in c1:
            if(i == s):
                if(c1[s] != c2[s][4]+1):
                    r = False
            else:
                if(c1[i] > c2[i][4]):
                    r = False
    else:
        for i in c1:
            if(c1[i] != c2[i][4]):
                r = False
    return r


def t_recieve(conn,addr):
    global view,state,leader,myid,iclock,newm,myport,mynick,wQ,dQ,dQn
    data = conn.recv(8192)
    #print data
    #print("\n("+addr[0]+":"+str(addr[1])+") > "+data+"\n")
    d = data.split(",")
    if(d[0] == "ping" and len(d) == 1):
        conn.send("ack")
    else:
        if(state == -1):
            if(d[0] == "join" and len(d) == 5 and d[2].isdigit()):
                myid = d[1]
                dQn = int(d[4])
                ff = d[3].strip().split(" ")
                newm = (myid,"127.0.0.1",myport,mynick,True,0)
                for i in xrange(0,int(d[2])):
                    view[ff[5*i]] = [ff[5*i+1],int(ff[5*i+2]),ff[5*i+3],True,int(ff[5*i+4])]
                conn.send("ack")
                print("\n > joined \n > ")
                state = 2
            else:
                conn.send("nak,bad request")
        else:
            if(leader == myid):
                if(d[0] == "msg" and len(d) == 4):
                    #msg,id,clock(id time id time),string
                    if(state == 0 or state == 1):
                        #"handle msg"
                        mcs = d[2]
                        d[2] = mcs.split(" ")
                        mclock = {}
                        for i in xrange(0,len(d[2]),2):
                            mclock[d[2][i]] = int(d[2][i+1])
                        if(cmpClock(mclock,view,d[1])):
                            view[d[1]][4] = mclock[d[1]]
                            getorder(mcs,data,view[d[1]],mclock)
                            dev = True
                            while(dev):
                                dev = False
                                for data in wQ:
                                    d = data.split(",")
                                    mcs = d[2]
                                    d[2] = mcs.split(" ")
                                    mclock = {}
                                    for i in xrange(0,len(d[2]),2):
                                        mclock[d[2][i]] = int(d[2][i+1])
                                    if(cmpClock(mclock,view,d[1])):
                                        view[d[1]][4] = mclock[d[1]]
                                        getorder(mcs,data,view[d[1]],mclock)
                                        dev = True
                        else:
                            wQ.append(data)
                        if(state == 2):
                            vc = {}
                            for i in view:
                                vc[i] = view[i][4]
                            if(cmpClock(iclock,view,False)):
                                view[myid][3]= True
                            r = True
                            for i in view:
                                if(view[i][3] == False):
                                    r = False
                                    break
                            if(r):
                                state = 3
                                ginfo = newm[0]+","+str(len(view))+","
                                for i in view:
                                    ginfo += i +" "+ view[i][0]+" "+ str(view[i][1])+" "+ view[i][2]+" "+ str(view[i][4]) + " "
                                d = s_send_message(newm[1:3],"join,"+ginfo, True).split(",")
                                view[myid][3] = True
                                if(d[0] == "ack" and len(d) == 1):
                                    view[newm[0]] = [newm[1],newm[2],newm[3],True,0]
                                    sendtoall("join,"+myid+",success")
                                else:
                                    sendtoall("join,"+myid+",reject")
                                state = 0
                    elif(state == 3):
                        wQ.append(data)
                    else:
                        conn.send("nak, bad request")
                elif(d[0] == "join" and len(d) == 3 and d[1].isdigit()):
                    if(state == 0):
                        state = 1
                        view[myid][3] = False
                        nid = genGroupId(view)
                        newm = (nid, addr[0], int(d[1]), d[2])
                        iclock={}
                        iclock[myid] = view[myid][4]
                        sendtoall("join,"+myid+","+nid+","+addr[0]+","+d[1]+","+d[2])
                        conn.send("ack")
                        if(len(view) == 1):
                            state = 3
                            ginfo = newm[0]+","+str(len(view))+","
                            for i in view:
                                ginfo += i +" "+ view[i][0]+" "+ str(view[i][1])+" "+ view[i][2]+" "+ str(view[i][4]) + " "
                            ginfo = ginfo.strip() + ","+str(dQn)
                            d = s_send_message(newm[1:3],"join,"+ginfo, True).split(",")
                            view[myid][3] = True
                            if(d[0] == "ack" and len(d) == 1):
                                view[newm[0]] = [newm[1],newm[2],newm[3],True,0]
                                sendtoall("join,"+myid+",success")
                            state = 0
                    else:
                        wQ.append(data)
                elif(d[0] == "ack" and len(d)==3 and d[2].isdigit()):
                    if(state == 1):
                        view[d[1]][3]= False
                        iclock[d[1]] = int(d[2])
                        r = True
                        for i in view:
                            if(view[i][3] == True):
                                r = False
                                break
                        if(r):
                            state = 2
                            r = ""
                            for i in iclock:
                                r += i +" "+ str(iclock[i])+" "
                            r = "clock,"+r.strip()
                            sendtoall(r)
                            vc = {}
                            for i in view:
                                vc[i] = view[i][4]
                            if(cmpClock(iclock,view,False)):
                                view[myid][3]=True
                            
                    else:
                        conn.send("nak,bad request")
                elif(d[0] == "ack" and len(d) == 2):
                    if(state == 2):
                        view[d[1]][3]= True
                        r = True
                        for i in view:
                            if(view[i][3] == False):
                                r = False
                                break
                        if(r):
                            state = 3
                            ginfo = newm[0]+","+str(len(view))+","
                            for i in view:
                                ginfo += i +" "+ view[i][0]+" "+ str(view[i][1])+" "+ view[i][2]+" "+ str(view[i][4]) + " "
                            ginfo = ginfo.strip() + ","+str(dQn)
                            d = s_send_message(newm[1:3],"join,"+ginfo, True).split(",")
                            view[myid][3] = True
                            if(d[0] == "ack" and len(d) == 1):
                                view[newm[0]] = [newm[1],newm[2],newm[3],False,0]
                                sendtoall("join,"+myid+",success")
                            else:
                                sendtoall("join,"+myid+",reject")
                            state = 0
                    else:
                        conn.send("nak,bad request")
                else:
                    conn.send("nak,bad request")
            else:
                if(d[0] == "order" and state >= 0 and len(d) == 4 and d[1] == leader):
                    mcs = d[2]
                    if(dQ.get(mcs) == None):
                        dQ[mcs] = [int(d[3]),None]
                    else:
                        dQ[mcs][0] = int(d[3])
                        if(dQn == dQ[mcs][0]):
                            d = dQ[mcs][1].split(",")
                            print("\n"+view[d[1]][2]+" > "+d[3]+"\n > ")
                            dQn += 1
                        dqc = True
                        while(dqc):
                            dqc = False
                            for i in dQ:
                                if(dQ[i][0] == dQn):
                                    d = dQ[i][1].split(",")
                                    print("\n" + view[d[1]][2]+" > "+d[3] + "\n > ")
                                    dQn += 1
                                    dqc = True
                elif(d[0] == "join" and len(d) == 6 and d[4].isdigit() and d[1] == leader):
                    if(state == 0):
                        state = 1
                        newm = (d[2], d[3], int(d[4]), d[5])
                        send_message(view[leader],"ack,"+myid+","+str(view[myid][4]))
                    else:
                        wQ.append(data)
                elif(d[0] == "clock" and len(d) == 2):
                    if(state == 1):
                        d[1] = d[1].split(" ")
                        iclock = {}
                        for i in xrange(0,len(d[1]),2):
                            iclock[d[1][i]] = int(d[1][i+1])
                        vc = {}
                        for i in view:
                            vc[i] = view[i][4]
                        if(cmpClock(iclock,view,False)):
                            s_send_message(view[leader],"ack,"+myid)
                            state = 2
                    else:
                        conn.send("nak","bad request")
                elif(d[0] == "join" and len(d) == 3 and d[1] == leader):
                    if(state == 2):
                        if(d[2] == "success"):
                            view[newm[0]] = [newm[1],newm[2],newm[3],True,0]
                            state = 0
                        elif(d[2] == "reject"):
                            state = 0
                        else:
                            conn.send("nak,bad request")
                    else:
                        conn.send("nak,bad request")
                elif(d[0] == "msg" and len(d) == 4):
                    #msg,id,clock,string
                    if(state == 0 or state == 1):
                        #"handle msg"
                        mcs = d[2]
                        d[2] = mcs.split(" ")
                        mclock = {}
                        for i in xrange(0,len(d[2]),2):
                            mclock[d[2][i]] = int(d[2][i+1])
                        if(cmpClock(mclock,view,d[1])):
                            view[d[1]][4] = mclock[d[1]]
                            getorder(mcs,data,view[d[1]],mclock)
                            dev = True
                            while(dev):
                                dev = False
                                for data in wQ:
                                    d = data.split(",")
                                    mcs = d[2]
                                    d[2] = mcs.split(" ")
                                    mclock = {}
                                    for i in xrange(0,len(d[2]),2):
                                        mclock[d[2][i]] = int(d[2][i+1])
                                    if(cmpClock(mclock,view,d[1])):
                                        view[d[1]][4] = mclock[d[1]]
                                        getorder(mcs,data,view[d[1]],mclock)
                                        dev = True
                        else:
                            wQ.append(data)
                        if(state == 1):
                            vc = {}
                            for i in view:
                                vc[i] = view[i][4]
                            if(cmpClock(iclock,view,False)):
                                s_send_message(view[leader],"ack")
                                state = 2
                    elif(state == 2):
                        wQ.append(data)
                    else:
                        conn.send("nak, bad request")
                else:
                    conn.send("nak, bad request")
            
    conn.close()
ilock = threading.Lock()
intt = None

def getin(s):
    global intt,ilock
    tmp = raw_input(s)
    intt = tmp

def server_socket(port):
    global view,state,leader,myid,iclock,newm,myport,mynick,wQ,dQ,dQn,intt,ilock
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', port))
        s.listen(5)
    except socket.error, e:
        print("\nSocket Error !!",
            "Unable To Setup Local Socket. Port In Use\n > ")
        return
    while True:
        conn, addr = s.accept()
        ilock.acquire()
        t_recieve(conn,addr)
        ilock.release()
    s.close()

myport = int(raw_input("enter port > "))
start_new_thread(server_socket,(myport,))

q = 1
while q:
    text = raw_input(" > ")
    if(text == "log"):
        print view
        print state
        print dQ
        print dQn
        print "\n"
    if(state == -1):
        if(text == "new"):
            d = s_send_message(rv,"new,"+str(myport),True)
            if(d != False):
                d = d.split(",")
                if(d[0] == "ack" and len(d) == 2):
                    text = raw_input("Enter nick > ")
                    mynick = text
                    print("\nsuccess > group id: "+d[1]+"\n > ")
                    view[d[1]] = ["127.0.0.1", myport, text, True, 0]
                    leader = d[1]
                    state = 0
                    myid=leader
                else:
                    print("\nfailure > server response: "+d+"\n > ")
            else:
                print("\nfailure > msg not sent\n > ")
        elif(text == "join"):
            text = raw_input("enter group id > ")
            t2 = raw_input("enter nick > ")
            mynick=t2
            d = s_send_message(rv,"join,"+str(myport)+","+text, True)
            if(d != False):
                d = d.split(",")
                if(d[0] == "ack" and len(d) == 3 and d[2].isdigit()):
                    dd = s_send_message((d[1],int(d[2])), "join,"+str(myport)+","+t2, True)
                    if(dd != "ack"):
                        print("\nfailure > rejected\n > ")
                    else:
                        print("\ninit > request initiated\n")
                        leader = text
            else:
                print("\nfailure > msg not sent\n > ")
    elif(state == 0):
        if(text == "msg"):
            ilock.acquire()
            text = raw_input("msg > ").replace(",",";")
            vcs = ""
            view[myid][4] += 1
            for i in view:
                vcs+=i+" "+str(view[i][4])+" "
            view[myid][4] -= 1
            sendtoall_m("msg,"+myid+","+vcs.strip()+","+text)
            ilock.release()