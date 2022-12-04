import sys
import time

from Airport import Airport


class Graph:

    def __init__(self, l: float=0.85, threshold: float=1e-8):
        self.all_airports: list[Airport] = [] # list of Airport
        self.airport_lookup: dict[str, Airport] = dict() # hash key IATA code -> Airport
        self.l = l
        self.threshold = threshold
    
    @property
    def n(self) -> int:
        return len(self.all_airports)

    def readAirports(self, fd):
        print("Reading Airport file from {0}".format(fd))
        airportsTxt = open(fd, "r", encoding='utf-8')
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
                self.all_airports.append(a)
                self.airport_lookup[a.code] = a
        airportsTxt.close()
        print(f"There were {cont} Airports with IATA code")

    def readRoutes(self, fd):
        print("Reading Routes file from {fd}")
        routesTxt = open(fd, "r", encoding='utf-8')
        for line in routesTxt.readlines():
            try:
                temp = line.split(',')
                FROM = temp[2]
                TO = temp[4]
                if FROM not in self.airport_lookup or TO not in self.airport_lookup:
                    raise Exception('Airport not in list')
                from_airport = self.airport_lookup[FROM]
                to_airport = self.airport_lookup[TO]
                if TO in from_airport.outgoing:
                    from_airport.outgoing[TO] += 1
                else:
                    from_airport.outgoing[TO] = 1
                if FROM in to_airport.incoming:
                    to_airport.incoming[FROM] += 1
                else:
                    to_airport.incoming[FROM] = 1
            except Exception as inst:
                pass
        routesTxt.close()

    def vertices_init(self) -> None:
        for airport in self.all_airports:
            airport.current_page_rank = airport.previous_page_rank = 1/self.n
            airport.update_outweight()
    
    def calculate_page_rank(self) -> None:
        while True:
            # Calculate the sum of the page ranks of the sink nodes, simulating the random probability of hopping to those pages
            # This will in effect be the same as if every sink node had an edge to all other nodes
            sink_sum = sum([airport.previous_page_rank for airport in self.all_airports if len(airport.outgoing) == 0])
            for airport in self.all_airports:
                rank_sum = 0
                for airport_code, weight in airport.incoming.items():
                    incoming_airport = self.airport_lookup[airport_code]
                    rank_sum += incoming_airport.previous_page_rank * weight / incoming_airport.outweight
                airport.current_page_rank = self.l * rank_sum + (1 - self.l) / self.n + self.l * sink_sum / self.n
            page_rank_sum = sum([airport.previous_page_rank for airport in self.all_airports])
            if (abs(page_rank_sum - 1) > 1e-6):
                raise Exception(f'Page rank sum != 1. Got {page_rank_sum}')
            if all([abs(airport.current_page_rank - airport.previous_page_rank) < self.threshold for airport in self.all_airports]):
                break
            self.update_ranks()
    
    def update_ranks(self) -> None:
        for airport in self.all_airports:
            airport.previous_page_rank = airport.current_page_rank

    def start_main(self) -> None:
        self.readAirports("airports.txt")
        self.readRoutes("routes.txt")
        self.vertices_init()
        airports_without_outgoing = [airport for airport in self.all_airports if len(airport.outgoing) == 0]
        print(f'Number of airports: {self.n}')
        print(f'Number of airports without outgoing routes: {len(airports_without_outgoing)}')
        time1 = time.time()
        self.calculate_page_rank()
        time2 = time.time()
        print(f'Page rank calculation took {time2 - time1} seconds')
        self.all_airports.sort(key=lambda x: x.current_page_rank, reverse=True)
        for airport in self.all_airports[:10]:
            print(airport)
    
    def start_tests(self) -> list[Airport]:
        if len(self.all_airports) == 0:
            self.readAirports("airports.txt")
            self.readRoutes("routes.txt")
        self.vertices_init()
        self.calculate_page_rank()
        self.all_airports.sort(key=lambda x: x.current_page_rank, reverse=True)
        return self.all_airports

def main(argv=None):
    l_values = [0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    threshold_values = [1e-5, 1e-8, 1e-10, 1e-12, 1e-15]
    results = []
    g = Graph()
    for l in l_values:
        for threshold in threshold_values:
            print(f'Running with l = {l} and threshold = {threshold}')
            g.l = l
            g.threshold = threshold
            results.append(g.start_tests())

if __name__ == "__main__":
    sys.exit(main())