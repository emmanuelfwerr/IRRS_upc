#!/usr/bin/python

from collections import namedtuple
from sklearn.metrics.pairwise import cosine_similarity
import time
import sys
import random
import numpy as np
import sklearn as skl
import pandas as pd

class Route:
    def __init__ (self, origin=None, destination=None):
        self.origin = origin # write appropriate value
        self.destination = destination

    def __repr__(self):
        return f"{self.origin}-{self.destination}"
        

class Edge:
    def __init__ (self, origin=None, destination=None, weight=None):
        self.origin = origin # write appropriate value
        self.destionation = destination
        self.weight = weight # write appropriate value

    def __repr__(self):
        return "{0} {1} {2}".format(self.origin, self.destination, self.weight)
        
    ## write rest of code that you need for this class

class Airport:
    def __init__ (self, iden=None, name=None, pageindex=None):
        self.code = iden
        self.name = name
        self.routes = []
        self.routeHash = dict()
        self.outweight = 0
        self.pageIndex = pageindex
        self.index = 0

    def __repr__(self):
        return f"{self.code}\t{self.pageIndex}\t{self.name}"

routeList = [] # list of Routes
routeHash = dict() # hash of routes to ease the match
edgeList = [] # list of Edge
edgeHash = dict() # hash of edge to ease the match
airportList = [] # list of Airport
airportHash = dict() # hash key IATA code -> Airport

def readAirports(fd):
    print("Reading Airport file from {0}".format(fd))
    airportsTxt = open(fd, "r");
    cont = 0
    for line in airportsTxt.readlines():
        a = Airport()
        try:
            temp = line.split(',')
            if len(temp[4]) != 5 :
                raise Exception('not an IATA code')
            a.name=temp[1][1:-1] + ", " + temp[3][1:-1]
            a.code=temp[4][1:-1]
            a.pageIndex=int(temp[0])
        except Exception as inst:
            pass
        else:
            a.index = cont
            cont += 1
            airportList.append(a)
            airportHash[a.code] = a #this is the output of that function
    airportsTxt.close()
    print(f"There were {cont} Airports with IATA code")


def readRoutes(fd):
    print("Reading Routes file from {fd}")
    routesTxt = open(fd, "r");
    cont = 0
    for line in routesTxt.readlines():
        a = Route()
        try:
            temp = line.split(',')
            if len(temp[2]) != 3 :
                raise Exception('not an IATA code')
            #get rid of route that link airport that are not in our database
            if (temp[2]) not in airportHash.keys() :
                raise Exception('not in airport list')
            if (temp[4]) not in airportHash.keys() :
                raise Exception('not in airport list')
            a.origin=temp[2]
            a.destination=temp[4]
            
        except Exception as inst:
            pass
        else:
            cont += 1
            routeList.append(a)
            #routeHash[a.origin] = a
    
    routesTxt.close()
    print(f"There were {cont} Routes with IATA code")
    

def createEdge() :
    #create a list of unique airports
    list_unique_airports = []
    for obj in routeList:
        list_unique_airports.append((obj.origin, obj.destination))
    # get rid of duplicates
    list_unique_airports = list(dict.fromkeys(list_unique_airports))
    
    #go through list of airport and find their weights
    for obj in list_unique_airports:
        edge = Edge()
  
        listupdate = [x if x.origin == obj[0] and x.destination == obj[1] else None for x in routeList]
        listupdate = list(dict.fromkeys(listupdate))
        const = len(listupdate) - listupdate.count(None)
        
        edge.origin = obj[0]
        edge.destination = obj[1]
        edge.weight = const
        edgeList.append(edge)
        #edgeHash[edge.origin] = edge
    
    print(f"The EdgeList was succesfully created in variable edgeList")
   
    
def addRoutes() :
    for a in airportList :
        e = Edge()
        listRoutes = []
        listRoutes = [i for i in edgeList if i.destination == a.code]
        
        listoutweight = [x for x in routeList if x.origin == a.code]
        outweight = len(listoutweight) - listoutweight.count(None)
        
        a.routes = listRoutes
        a.outweight = int(outweight)
    
    #the routes that start or arrive to an airport that is not listed in the file airports have not been added to the airport document
    print(f"The AirportList was succesfully filled by its corresponding edges")
    
    
def computePageRanks():
    # write your code
    n = len(airportList)
    Q1 = np.full(n, 0).reshape(-1, 1)
    Q = np.full(n, 1/n).reshape(-1, 1)
    P = np.full(n, 1/n).reshape(-1, 1)
    L = 0.85
    iteration = 0
    
    #we are going to distribute the pagerank of the isolated airport to the other ones
    airportAlone = []
    for i in airportList :
        if i.outweight < 1 :
            airportAlone.append(i)
    others = len(airportAlone)/n
    
    
    while (not( np.mean(np.abs((P-Q1)/P)) < 0.05 )) :
        iteration +=1
        
        for i in airportList :
            elQ = [(P[airportHash[edge.origin].index] * edge.weight)/ airportHash[edge.origin].outweight for edge in i.routes]
            sumQ = sum(elQ)
            Q[i.index]= L*(sumQ+ others) + (1-L)/n
        
        Q1 = P.copy()
        P = Q.copy()
   
    return iteration, P
    
def outputPageRanks(P):
    df = pd.DataFrame(columns = ['PageRank', 'Airport_name', 'Airport_info'])
    for i in range(len(P)) :
        df = df.append({'PageRank': P[i], 'Airport_name': airportList[i].code, 'Airport_info' : airportList[i].name}, ignore_index=True)
    df = df.sort_values(by='PageRank', ascending=False)
    print(df)
    df.to_csv('outputPageRanks.csv')

def main(argv=None):

    readAirports("airports.txt")
    readRoutes("routes.txt") #just extracted the connection without adding the weights
    createEdge()
    addRoutes()
    time1 = time.time()
    iterations, P = computePageRanks()
    time2 = time.time()
    outputPageRanks(P)
    print("#Iterations:", iterations)
    print("Time of compute PageRanks():", time2-time1)


if __name__ == "__main__":
    sys.exit(main())
