import thread
import threading
import time
import sys
import multiprocessing
from multiprocessing import Pipe
import Queue
import contextlib

def count(list,conn):
        start = int(round(time.time() * 1000))
        dit = {}
        print "counting words"
        for word in list:    
            if word in dit.keys():
                dit[word] = dit[word] + 1
            else:
                dit[word] = 1
        conn.send(dit)
        
        print "count time:",int(round(time.time() * 1000)) - start
        
def getWordsFromFile(numThreads, fileName):
    lists = []
    start = int(round(time.time() * 1000))
    for i in range(int(numThreads)):
        k = []
        lists.append(k)
    file = open(fileName, "r")  # uses .read().splitlines() instead of readLines() to get rid of "\n"s
    all_words = map(lambda l: l.split(" "), file.read().splitlines()) 
    all_words = make1d(all_words)
    cur = 0
    for word in all_words:
        lists[cur].append(word)
        if cur == len(lists) - 1:
            cur = 0
        else:
            cur = cur + 1
    print "getting words took:",int(round(time.time() * 1000)) - start
    return lists

def make1d(list):
    newList = []
    for x in list:
        newList += x
    return newList

def printDict(dit):# prints the dictionary nicely
    for key in sorted(dit.keys()):
        print key, ":", dit[key]  

def fuseDicts(listOfConn):
   start = int(round(time.time() * 1000))
   prime = listOfConn[0].recv()
   for c in listOfConn[1:len(listOfConn)]:
      sec = c.recv()
      for k in sec.keys():
         if k in prime.keys():
            prime[k] = prime[k]+sec[k]
         else:
            prime[k] = 1
   print "fuseDicts time:" , int(round(time.time() * 1000)) - start
   for k in listOfConn:
      k.close()
   return prime
def getTestData(maxNumOfProcs, fileName, numOfIterations):
   outF = file("results.txt;","w")
   for i in range(maxNumOfProcs):
      totalTime = 0
      x = i +1
      for k in range(numOfIterations):     
         start = getCurrTime()
         ditList = []
         for p in range(x):
             ditList.append({})
         zipOfProccConn = createProcList(maxNumOfProcs,getWordsFromFile(x,fileName))
         procList = zipOfProccConn[0]
         pipeList = zipOfProccConn[1]
         map(lambda l: l.start(),procList)
         dict = fuseDicts(pipeList)
         fin = getCurrTime() - start
         totalTime = totalTime + fin
      outF.write("%d,%d \n"%(x,totalTime/numOfIterations))



      
def createProcList(maxNumOfProcs,wordLists):
   procList = []
   pipeList = []
   for list in wordLists:
       p_conn, ch_conn = Pipe()
       procList.append(multiprocessing.Process(target=count,args=(list,ch_conn)))
       pipeList.append(p_conn)
   
   return (procList,pipeList)

 
def getCurrTime():
   return int(round(time.time()*1000))

if __name__=="__main__":
    print "Starting now"
    start = int(round(time.time() * 1000))
    ditList = []
    procList = []
    pipeList = []
    args = sys.argv
    if len(args) == 4:
       getTestData(int(args[1]),args[2],int(args[3]))
    else:
       numThreads = args[1]
       fileName = "" + args[2]
       for i in range(int(numThreads)):
          ditList.append({})
       wordLists = getWordsFromFile(numThreads, fileName)
       print "got words from file"
       starts = int(round(time.time() * 1000))
       for list in wordLists:
          p_conn, ch_conn = Pipe()
          procList.append(multiprocessing.Process(target=count,args=(list,ch_conn)))
          pipeList.append(p_conn)
       print "creating procs and pipes took",int(round(time.time() * 1000)) - starts
       starts = int(round(time.time() * 1000))
       map(lambda l: l.start(),procList)
       print "all procs to finish took:", int(round(time.time() * 1000)) - starts
       dict = fuseDicts(pipeList)
       fin = int(round(time.time() * 1000)) - start
       print "with", numThreads, "threads", "counting the words took :", fin, "ms"
       boole = input("enter 1 if you want to print the frequency of each word  ")
       if(boole):
          printDict(dict)
       print "done"
    
