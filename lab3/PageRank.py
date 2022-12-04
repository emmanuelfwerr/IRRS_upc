#!/usr/bin/python

from collections import namedtuple
import time
import sys

import numpy as np
import numpy.linalg as la

class Edge:
    def __init__ (self, origin=None, weight=None):
        self.origin = origin # write appropriate value
        self.weight = weight # write appropriate value

    def __repr__(self):
        return "edge: {0} {1}".format(self.origin, self.weight)
        
    ## write rest of code that you need for this class

class Airport:
    def __init__ (self, iden=None, name=None):
        self.code = iden
        self.name = name
        self.incoming = dict()
        self.outgoing = dict()
        self.outweight = None
        self.current_page_rank = 0
        self.previous_page_rank = 0

    def __repr__(self):
        return f"{self.code}\t{self.current_page_rank}\t{self.name}"
    
    def update_outweight(self) -> None:
        self.outweight = sum(self.outgoing.values())

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
        except Exception as inst:
            pass
        else:
            cont += 1
            airportList.append(a)
            airportHash[a.code] = a
    airportsTxt.close()
    print(f"There were {cont} Airports with IATA code")


def readRoutes(fd):
    print("Reading Routes file from {fd}")
    # write your code

def computePageRanks():
    ... # write your code

def outputPageRanks():
    ... # write your code

def main(argv=None):
    readAirports("airports.txt")
    readRoutes("routes.txt")
    time1 = time.time()
    iterations = computePageRanks()
    time2 = time.time()
    outputPageRanks()
    print("#Iterations:", iterations)
    print("Time of computePageRanks():", time2-time1)


if __name__ == "__main__":
    sys.exit(main())
