#!/usr/bin/python

from collections import namedtuple
import time
import sys
import random

import numpy as np
import numpy.linalg as la

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
        #self.outweight = # write appropriate value
        self.pageIndex = pageindex

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
            a.pageIndex=temp[0]
        except Exception as inst:
            pass
        else:
            cont += 1
            a.pageIndex = cont
            airportList.append(a)
            airportHash[a.code] = a #this is the output of that function
    airportsTxt.close()
    print(f"There were {cont} Airports with IATA code")


def readRoutes(fd):
    print("Reading Routes file from {fd}")
    routesTxt = open(fd, "r")
    cont = 0
    for line in routesTxt.readlines():
        a = Route()
        try:
            temp = line.split(',')
            if len(temp[2]) != 3:
                raise Exception('not an IATA code')
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
        
        #if an airport does have incoming edge, we have a virtual edge with weight 0
        if not listRoutes :
            el = random.choice(airportList)
            e.origin = el.code
            e.destination = a.code
            e.weight = 0
            listRoutes.append(e)
        
        a.routes = listRoutes
    
    #the routes that start or arrive to an airport that is not listed in the file airports have not been added to the airport document
    print(f"The AirportList was succesfully filled by its corresponding edges")
    
    
def computePageRanks():
    # write your code
    n = len(airportList)
    P = np.full(n, 1/n)
    L = 0.85
    
'''    while (not P = Q vector cosine similarity < epsilon) {
        Q = the all-0 n-vector;
        for i in airportList {
            #trouver j
            
            airportHash
            Q[i.pageIndex]= L* sum{ P[j] * w(j,i)/out(j):
                        there is an edge (j,i) in G }
                  + (1-L)/n;
        }
        P = Q;
    }'''

def outputPageRanks():
    ...
    # write your code

def main(argv=None):

    readAirports("airports.txt")
    readRoutes("routes.txt") #just extracted the connection without adding the weights
    createEdge()
    addRoutes()
    time1 = time.time()
    iterations = computePageRanks()
    time2 = time.time()
    outputPageRanks()
    print("#Iterations:", iterations)
    print("Time of compute PageRanks():", time2-time1)


if __name__ == "__main__":
    sys.exit(main())
