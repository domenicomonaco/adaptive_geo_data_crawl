#Adaptive Geolocated-data crawler: intelligent adaptive crawler for Google Place
#Monaco, D. (2019). An adaptive agent for Google Place crawling (No. 934). EasyChair
#3-clause BSD license
#Domenico Monaco, Tecnologie per persone, http://domenicomonaco.it

import sys
import math
import json
from StringIO import StringIO
import pymongo

from pprint import pprint
import googlemaps
import time
from datetime import datetime
from pymongo import MongoClient

try:
    from types import SimpleNamespace as Namespace
except ImportError:
    # Python 2.x fallback
    from argparse import Namespace

class Ring:
    def __init__(self, id_ring, (root_lat, root_long), radius_hexagon):

        self.__grid = dict()
        self.f_cost_hexagon = math.sqrt(3)/2;

        self.movment_factors = {
            ## ["MOVEMENT NAME", [Multiplier lat, Multiplier lon]],
            "NN": [math.sqrt(3)/2, 0],
            "NE": [0.5, math.sqrt(3)/2],
            "NW": [0.5, -math.sqrt(3)/2],
            "SS": [-(math.sqrt(3)/2), 0],
            "SE": [-0.5, (math.sqrt(3)/2)],
            "SW": [-0.5, -(math.sqrt(3)/2)]
        }

        self.hexagons_pos = None
        self.hexagons_pos = dict()

        #0.00898311174 more precice
        self.eahrt_dPI = 0.0000089

        self.id_ring = id_ring;
        self.radius_hexagon = radius_hexagon
        self.root_pos = (root_lat, root_long)

        if self.id_ring == 0:
            self.n_hexagons = 1
        elif self.id_ring >= 1:
            self.n_hexagons = self.id_ring * 6

            ##self.meters_shift_first_hexagon = (self.radius_hexagon * self.id_ring)

    def init_ring_JSON(self):
        self.__grid["hexagons_pos"] = dict()
        self.__grid["n_hexagons"] = self.n_hexagons
        self.__grid["root_pos"] = self.root_pos
        self.__grid["radius_hexagon"] = self.radius_hexagon
        self.__grid["time_generated"] = datetime.now()
        self.__grid["id_ring"] = self.id_ring
        return 1

    def get_ring_JSON(self):
        return self.__grid

    def move(self, dir, location_start=None):
        if location_start == None:
            start_lat = self.root_pos[0]
            start_lon = self.root_pos[1]
        else:
            start_lat = location_start[0]
            start_lon = location_start[1]

        a = self.radius_hexagon * self.movment_factors[dir][0]
        coef = a * self.eahrt_dPI
        if location_start == None:
            coef = coef * (self.id_ring * 2)
        else:
            coef = coef * 2

        if coef >= 0:
            new_lat = start_lat + coef
        else:
            coef = coef * -1
            new_lat = start_lat - coef

        a = self.radius_hexagon * self.movment_factors[dir][1]
        coef = a * self.eahrt_dPI
        if location_start == None:
            coef = coef * (self.id_ring * 2)
        else:
            coef = coef * 2


        if coef >= 0:
            new_long = start_lon + coef / math.cos(start_lat * 0.018)
        else:
            coef = coef * -1
            new_long = start_lon - coef / math.cos(start_lat * 0.018)

        return(new_lat, new_long)


    def draw_ring(self):
        self.hexagons_pos = None
        self.hexagons_pos = dict()
        item_hexagon = 0

        # LATO NORD NN, punto di partenza
        # nel caso di ring 0 , root e Punto di paartenza coincidono
        if self.n_hexagons == 1:

            self.hexagons_pos[str(item_hexagon)] = self.root_pos
            self.__grid["hexagons_pos"] = self.hexagons_pos

            return self.hexagons_pos

        elif self.n_hexagons >= 2:
            self.hexagons_pos[str(item_hexagon)] = self.move("NN")

        # LATO NORD-EST, scorrimento dal esagono 0

        for i in range((self.n_hexagons / 6)):
            #print "SE"
            item_hexagon = item_hexagon + 1
            self.hexagons_pos[str(item_hexagon)] = self.move("SE", self.hexagons_pos[str(item_hexagon-1)])


        # LATO SUD-EST, scorrimento dal esagono 0
        for i in range((self.n_hexagons / 6)):
            item_hexagon = item_hexagon + 1
            #print "SS"
            self.hexagons_pos[str(item_hexagon)] = self.move("SS", self.hexagons_pos[str(item_hexagon-1)])


        # LATO SUD-SUD
        for i in range((self.n_hexagons / 6)):
            item_hexagon = item_hexagon + 1
            #print "SW"
            self.hexagons_pos[str(item_hexagon)] = self.move("SW", self.hexagons_pos[str(item_hexagon-1)])

        # LATO SUD-OVEST, scorrimento dal esagono 0
        for i in range((self.n_hexagons / 6)):
            #print "NW"
            item_hexagon = item_hexagon + 1
            self.hexagons_pos[str(item_hexagon)] = self.move("NW", self.hexagons_pos[str(item_hexagon-1)])

        # LATO NORD-OVEST, scorrimento dal esagono 0
        for i in range((self.n_hexagons / 6)):
            item_hexagon = item_hexagon + 1
            #print "NN"
            self.hexagons_pos[str(item_hexagon)] = self.move("NN", self.hexagons_pos[str(item_hexagon-1)])

        if self.n_hexagons>=12:
            # LATO NORD-OVEST, scorrimento dal esagono 0
            for i in range((self.n_hexagons / 6)-1):
                item_hexagon = item_hexagon + 1
                #print "NE"
                self.hexagons_pos[str(item_hexagon)] = self.move("NE", self.hexagons_pos[str(item_hexagon - 1)])

        self.__grid["hexagons_pos"] = self.hexagons_pos
        return self.hexagons_pos
        
