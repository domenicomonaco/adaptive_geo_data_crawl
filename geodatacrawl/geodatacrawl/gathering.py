#Adaptive Geolocated-data crawler: intelligent adaptive crawler for Google Place
#Monaco, D. (2019). An adaptive agent for Google Place crawling (No. 934). EasyChair
#3-clause BSD license
#Domenico Monaco, Tecnologie per persone, http://domenicomonaco.it

import sys
import math
import time
from datetime import datetime
from pymongo import MongoClient
import sys
import math
import json
from StringIO import StringIO
import pymongo

from pprint import pprint
from random import randint

from geodatacrawl.movement import Ring
from geodatacrawl.google import GoogleReq

try:
    from types import SimpleNamespace as Namespace
except ImportError:
    # Python 2.x fallback
    from argparse import Namespace


class Gathering:

    def __init__(self,
                 (root_lat, root_long),
                 radius_hexagon,
                 sleep=5,
                 modedetails=0,
                 startring=0,
                 endring=0,
                 city = 'city',
                 keylist=('EXAMPLE_OF_GOOGLE_KEY1', 'EXAMPLE_OF_GOOGLE_KEY2', 'EXAMPLE_OF_GOOGLE_KEY3')
                 ):

        self._gathering_stat_JSON = dict()
        self._lastrequest = None
        
        self._list_key = keylist

        self.time_to_sleep_exception = 5
        self._alert_number_places_hexagon = 59
        self._add_radius = 5
        self.__min_radius = 10

        self._sleep = sleep
        self._used_key = randint(0, len(self._list_key)-1)

        self._current_grid = 0
        self._current_hexagon = 0
        self._current_ring = 0
        self._type_ring = "main"
        self._total_hexagon_processed = 0

        self.__radius = radius_hexagon
        self.__min_radius = 10
        self.__root_location = (root_lat, root_long)

        datetime_execution = time.strftime("%d_%m_%Y_%H_%M_%S")

        self.db_name_base = city
        
        self.__db_name = self.db_name_base + "_pois_%s-%s" %(str(startring), str(endring))

        self.__coll_logs_name = "logs"
        self.__coll_gather_stats_name = "gather_stats"
        self.__coll_ring_name = "rings"
        self.__coll_requests_name = "requests_hexagons"
        self.__coll_nearby_places_name = "nearby_places"

        self.__mongoclient = MongoClient('mongodb://mongo', 27020)

        if modedetails == 0:
            self.__db = self.__mongoclient[self.__db_name]
            self.__coll_logs = self.__db[self.__coll_logs_name]
            self.__coll_nearby_places = self.__db[self.__coll_nearby_places_name]
            self.__coll_rings = self.__db[self.__coll_ring_name]
            self.__coll_requests = self.__db[self.__coll_requests_name]
            self.__coll_gather_stats = self.__db[self.__coll_gather_stats_name]
        else:
            self.__db_name_merged = "%s_pois_clean" %(str(self.db_name_base))
            self.__coll_nearby_places_name_merged = "nearby_places"
            self.__coll_detail_places_name_merged = "detail_places"
            self.__coll_detail_places_checked_name_merged = "detail_places_checked"
            self.__coll_detail_places_error_name = "detail_places_error"

            self.__db_merged = self.__mongoclient[self.__db_name_merged]

            self.__coll_nearby_places_merged = self.__db_merged[self.__coll_nearby_places_name_merged]
            self.__coll_detail_places_merged = self.__db_merged[self.__coll_detail_places_name_merged]
            self.__coll_detail_places_checked_merged = self.__db_merged[self.__coll_detail_places_checked_name_merged]
            self.__coll_detail_places_error = self.__db_merged[self.__coll_detail_places_error_name]

        return None

    def set_alert_number_places_hexagon(self, value):
        self._alert_number_places_hexagon = value
        return 1

    def set_add_radius(self, value):
        self._add_radius = value
        return 1

    def init_gathering_stat_JSON(self):

        #self.__coll_gather_stats.drop()


        out = self.__coll_gather_stats.find_one({"ID_LOG": "gathering_stat_JSON"})

        if out != None:
            self._gathering_stat_JSON = out
            del self._gathering_stat_JSON['_id']
            self.__coll_gather_stats.replace_one({"ID_LOG": "gathering_stat_JSON"}, self._gathering_stat_JSON)

        else:
            self._gathering_stat_JSON["ID_LOG"] = "gathering_stat_JSON"

            self._gathering_stat_JSON["sleep"] = self._sleep

            self._gathering_stat_JSON["root_location"] = self.__root_location

            self._gathering_stat_JSON["db_vars"] = dict()
            self._gathering_stat_JSON["db_vars"]["db_name"] = self.__db_name
            self._gathering_stat_JSON["db_vars"]["coll_logs_name"] = self.__coll_logs_name
            self._gathering_stat_JSON["db_vars"]["coll_ring_name"] = self.__coll_ring_name

            self._gathering_stat_JSON["db_vars"]["coll_requests_name"] = self.__coll_requests_name
            self._gathering_stat_JSON["db_vars"]["coll_nearby_places_name"] = self.__coll_nearby_places_name
            self._gathering_stat_JSON["db_vars"]["coll_nearby_places_name"] = self.__coll_nearby_places_name

            self._gathering_stat_JSON["grid"] = dict()
            self._gathering_stat_JSON["grid"]["generated_ring"] = 0
            self._gathering_stat_JSON["grid"]["radius"] = self.__radius

            self._gathering_stat_JSON["google_key"] = dict()
            self._gathering_stat_JSON["google_key"]["failed_key"] = 0
            self._gathering_stat_JSON["google_key"]["used_key"] = self._used_key

            self._gathering_stat_JSON["stats_request"] = dict()
            self._gathering_stat_JSON["stats_request"]["total_request"] = 0
            self._gathering_stat_JSON["stats_request"]["total_hex"] = 0
            self._gathering_stat_JSON["stats_request"]["total_places"] = 0
            self._gathering_stat_JSON["stats_request"]["avg_place_per_hexagon"] = 0

            self._gathering_stat_JSON["deep_alert_hexagon"] = list()

            self._gathering_stat_JSON["deep_alert_hexagon_checked"] = list()
            self._gathering_stat_JSON["deep_alert_hexagon_skipped"] = list()

            self.__coll_gather_stats.insert_one(self._gathering_stat_JSON)

        return 0

    ##############
    # START GRID #
    ##############
    def start_grid_update_stats(self, n_ring, start=0):
        timenow = datetime.now()
        self._gathering_stat_JSON["grid"]["total_ring"] = n_ring + 1 - start
        self._gathering_stat_JSON["grid"]["n_ring_finish"] = n_ring
        self._gathering_stat_JSON["grid"]["n_ring_start"] = start
        self._gathering_stat_JSON["grid"]["time_start"] = timenow
        return 1

    def start_grid_write_log(self, n_ring, start=0):
        timenow = datetime.now()
        print "\n" \
              "# Start Grid Ghathering:" \
                "\n" \
                "## n_ring: %d" \
                "\n" \
                "## total_ring: %d" \
                "\n" \
                "## start: %s" \
                % (n_ring, n_ring+1, str(timenow))
        return 1

    def start_grid_save_log(self, n_ring, start=0):
        timenow = datetime.now()
        self.__coll_logs.insert_one(
            {"comment": "Start Grid Ghathering", "time": timenow, "n_ring": n_ring}
        )
        return 1

    ##############
    # START RING #
    ##############
    def start_ring_write_log(self, i):
        timenow = datetime.now()
        print "\n" \
                "# Start Ring:" \
                "\n" \
                "## current_ring: %d" \
                "\n" \
                "## start: %s" \
                "\n" \
                "## PATH GRID: %d" \
                "\n" \
                % (i, timenow, self._current_grid)
        return 1

    def start_ring_save_log(self, i):
        timenow = datetime.now()
        self.__coll_logs.insert_one({"comment": "Start Ring", "time": timenow, "current_ring": i})
        return 1

    ##################
    # GENERATED RING #
    ##################
    def generated_ring_write_log(self, generated_ring):
        print "\n" \
                "# Generated Ring:" \
                "\n" \
                "## time_generated: %s" \
                "\n" \
                "## id_ring: %d" \
                "\n" \
                "## PATH_RING: %s" \
                "\n" \
                "## n_hexagons: %d" \
                "\n" \
                "## radius_hexagon: %d" \
                % (str(generated_ring["time_generated"]),
                   generated_ring["id_ring"],
                   "%s.%d.%d.%d" % (
                       self._type_ring, self._current_grid, self._current_ring, self._current_hexagon),
                   generated_ring["n_hexagons"],
                   generated_ring["radius_hexagon"])

        return 1

    def generated_ring_save_log(self, generated_ring):
        self.__coll_logs.insert_one(
            {
                "comment": "Generated Ring",
                "time": generated_ring["time_generated"],
                "current_ring": generated_ring["id_ring"],
                "n_hexagon": generated_ring["n_hexagons"],
                "radius_hexagon": generated_ring["radius_hexagon"]
            })
        return 1

    def generated_ring_update_stats(self, generated_ring):
        # UPDATE _gathering JSON
        self._gathering_stat_JSON["grid"]["generated_ring"] = generated_ring["id_ring"]
        return 1

    def save_requests(self):
        self.__coll_requests.insert_one(self._lastrequest.get_nearby_requests_JSON())
        return 1

    def save_nearby_locations(self):
        if bool(self._lastrequest.get_nearby_results_JSON()):
            self.__coll_nearby_places.insert_many(self._lastrequest.get_nearby_results_JSON())
        return 1

    def save_ring(self, ring_JSON):
        self.__coll_rings.insert_one(ring_JSON)
        return 1

    def write_log_finished_gathering(self):

        timenow = datetime.now()
        self._gathering_stat_JSON["grid"]["end"] = timenow

        print "\n" \
              "# Finish Gathering:" \
              "\n" \
              "## start: %s" \
              "\n" \
              "## end: %s" \
              "\n" \
              "## requested_ring: %d" \
              "\n" \
              "## generated_ring: %d" \
              % (str(self._gathering_stat_JSON["grid"]["time_start"]),
                 str(self._gathering_stat_JSON["grid"]["end"]),
                 self._gathering_stat_JSON["grid"]["total_ring"],
                 self._gathering_stat_JSON["grid"]["generated_ring"]
                 )
        return 1

    def update_stats(self, updateValueRequest):

        self._gathering_stat_JSON["stats_request"]["current_type_grid"] = "%s" % (
            self._type_ring)
        self._gathering_stat_JSON["stats_request"]["current_grid"] = "%s.%d" % (
            self._type_ring, self._current_grid)
        self._gathering_stat_JSON["stats_request"]["current_ring"] = "%s.%d.%d" % (
            self._type_ring, self._current_grid, self._current_ring)
        self._gathering_stat_JSON["stats_request"]["current_hexagon"] = "%s.%d.%d.%d" % (
            self._type_ring, self._current_grid, self._current_ring, self._current_hexagon)



        if updateValueRequest == 1:
            """
            % (str(self._lastrequest.get_nearby_requests_JSON()['_latlong']),
            str(self._lastrequest.get_nearby_requests_JSON()['_total_pages']),
            str(self._lastrequest.get_nearby_requests_JSON()['_total_places']),
            """

            self._gathering_stat_JSON["stats_request"]["total_request"] = self._gathering_stat_JSON["stats_request"][
                                                                              "total_request"] + \
                                                                          self._lastrequest.get_nearby_requests_JSON()[
                                                                              '_total_pages']
            self._gathering_stat_JSON["stats_request"]["total_hex"] = self._total_hexagon_processed

            self._gathering_stat_JSON["stats_request"]["total_places"] = \
                self._gathering_stat_JSON["stats_request"]["total_places"] + \
                self._lastrequest.get_nearby_requests_JSON()['_total_places']

            self._gathering_stat_JSON["stats_request"]["avg_place_per_hexagon"] = \
                float(self._gathering_stat_JSON["stats_request"]["total_places"] / self._gathering_stat_JSON["stats_request"]["total_hex"])

            self._gathering_stat_JSON["stats_request"]["avg_page_per_hexagon"] = self._gathering_stat_JSON["stats_request"]["total_request"] / self._gathering_stat_JSON["stats_request"]["total_hex"]



            if self._lastrequest.get_nearby_requests_JSON()['_total_places'] >= self._alert_number_places_hexagon:

                self._gathering_stat_JSON["deep_alert_hexagon"].append({
                    "lat_lon": self._lastrequest.get_nearby_requests_JSON()['_latlong'],
                    "total_place": self._lastrequest.get_nearby_requests_JSON()['_total_places'],
                    "alert_place": self._alert_number_places_hexagon,
                    "radius": self._lastrequest.get_nearby_requests_JSON()['_radius'],
                    "type_grid": self._type_ring,
                    "grid": self._current_grid,
                    "ring": self._gathering_stat_JSON["grid"]["generated_ring"],
                    "hexagon": self._current_hexagon,
                    "path_hexagon": "%s.%d.%d.%d" % (
                    self._type_ring, self._current_grid, self._current_ring, self._current_hexagon),
                    "gathered": "no"
                    })

        return 1


    def save_finished_gathering(self):
        self.__coll_gather_stats.replace_one({"ID_LOG": "gathering_stat_JSON"}, self._gathering_stat_JSON)
        return 1

    def write_log_request_hexagon(self):
        self.__coll_logs.insert_one(
            {
                "comment": "Save requests hexagon",
                "time": datetime.now()
             })
        return 1

    def save_request_hexagon(self, requests_output):
        self.__coll_requests.insert_one(requests_output)
        return 1

    """"
    def purge_database(self, drop=True):
        for db_name in self.__mongoclient.database_names():
            self.__mongoclient.drop_database(db_name)
    """

    def purge_collection(self, drop=True):
        for collection in self.__db.collection_names():
            self.__db.get_collection(collection).drop()

    def finished_request_write_log(self):

        print "\n" \
              "# Finish request on single Location:" \
              "\n" \
              "## location: %s" \
              "\n" \
              "## total pages: %s" \
              "\n" \
              "## total place: %s" \
              "\n" \
              "## time to sleep: %s" \
              "\n" \
              "## used key: %s" \
              "\n" \
              "## start time: %s" \
              "\n" \
              "## end time: %s" \
              "\n" \
              % ( str(self._lastrequest.get_nearby_requests_JSON()['_latlong']),
                  str(self._lastrequest.get_nearby_requests_JSON()['_total_pages']),
                  str(self._lastrequest.get_nearby_requests_JSON()['_total_places']),
                  str(self._lastrequest.get_nearby_requests_JSON()['_timesleep']),
                  str(self._lastrequest.get_nearby_requests_JSON()['_used_key']),
                  str(self._lastrequest.get_nearby_requests_JSON()['_start_time']),
                  str(self._lastrequest.get_nearby_requests_JSON()['_end_time']),
              )
        return 1

    def finished_request_save_log(self):

        self.__coll_logs.insert_one(
            {
                "comment": "Save the finished OK request",
                "time": datetime.now(),
                "location": str(self._lastrequest.get_nearby_requests_JSON()['_latlong']),
                "total pages": str(self._lastrequest.get_nearby_requests_JSON()['_total_pages']),
                "total places": str(self._lastrequest.get_nearby_requests_JSON()['_total_places']),
                "time to sleep": str(self._lastrequest.get_nearby_requests_JSON()['_timesleep']),
                "used key": str(self._lastrequest.get_nearby_requests_JSON()['_used_key']),
                "start time": str(self._lastrequest.get_nearby_requests_JSON()['_start_time']),
                "end time": str(self._lastrequest.get_nearby_requests_JSON()['_end_time']),
            })

        return 1

    def write_log_ERROR(self, error, generated_ring, i, r):

        print "\n" \
              "# ERROR: %s" \
              "\n" \
              "## location: %s" \
              "\n" \
              "## time to sleep: %s" \
              "\n" \
              "## used key: %s" \
                "\n" \
                "## ID NEW KEY: %d" \
              "\n" \
              "## start time: %s" \
              "\n" \
              "## id_ring: %s" \
              "\n" \
              "## PATH_RING: %s" \
              "\n" \
              "## n_hexagon: %s" \
              "\n" \
              "## item i ring time: %s" \
              "\n" \
              "## item r hexagon: %s" \
              "\n" \
              "## radius %s" \
              "\n" \
              % ( str(error),
                  str(self._lastrequest.get_nearby_requests_JSON()['_latlong']),
                  str(self._lastrequest.get_nearby_requests_JSON()['_timesleep']),
                  str(self._lastrequest.get_nearby_requests_JSON()['_used_key']),
                  self._used_key,
                  str(self._lastrequest.get_nearby_requests_JSON()['_start_time']),
                    str(generated_ring["id_ring"]),
                  "%s.%d.%d.%d" % (
                      self._type_ring, self._current_grid, self._current_ring, self._current_hexagon),
                    str(generated_ring["n_hexagons"]),
                    str(i),
                  str(r),
                  str(self._lastrequest.get_nearby_requests_JSON()['_radius'])
              )

        error = "Error %s" % str(error)

        self.__coll_logs.insert_one(
            {
                "comment": error,
                "time": datetime.now(),
                "location": str(self._lastrequest.get_nearby_requests_JSON()['_latlong']),
                "time to sleep": str(self._lastrequest.get_nearby_requests_JSON()['_timesleep']),
                "radius":str(self._lastrequest.get_nearby_requests_JSON()['_radius']),
                "used key": str(self._lastrequest.get_nearby_requests_JSON()['_used_key']),
                "start time": str(self._lastrequest.get_nearby_requests_JSON()['_start_time']),
                "item KEY time": str(self._used_key),
                "current_type_grid": "%s" % (
                    self._type_ring),
                "current_grid": "%s.%d" % (
                    self._type_ring, self._current_grid),
                "current_ring": "%s.%d.%d" % (
                    self._type_ring, self._current_grid, self._current_ring),
                "current_hexagon": "%s.%d.%d.%d" % (
                    self._type_ring, self._current_grid, self._current_ring, self._current_hexagon),
                "total_request": self._gathering_stat_JSON["stats_request"][
                                     "total_request"] + \
                                 self._lastrequest.get_nearby_requests_JSON()[
                                     '_total_pages']
            })

        self._gathering_stat_JSON["google_key"]["failed_key"] = self._gathering_stat_JSON["google_key"]["failed_key"] + 1
        self._gathering_stat_JSON["google_key"]["used_key"] = self._used_key

        return 1

    def change_key(self, generated_ring=None):

        time.sleep(self.time_to_sleep_exception)

        if self._used_key >= len(self._list_key) - 1:
            if generated_ring!=None:
                self.write_log_ERROR("FINISH VALID KEY, RE-START BY TOP ", generated_ring, self._current_ring,
                                 self._current_hexagon)
                self.update_stats(0)
                # self.write_log_finished_gathering()
                self.save_finished_gathering()

            #restart from 0
            self._used_key = 0

        else:
            self._used_key = self._used_key + 1
            if generated_ring != None:
                self.write_log_ERROR("NEW KEY, after exception", generated_ring, self._current_ring,
                                 self._current_hexagon)

        return 1

    def run(self, n_ring, start=0, custom_root=(0,0), type_ring="main"):

        print "######################"
        print "#TYPE OF GATHERING %s" % self._type_ring
        print "######################"

        # self.start_grid_save_log(n_ring, start)
        # self.start_grid_write_log(n_ring, start)

        for self._current_ring in range(start, n_ring+1, 1):

            self.start_ring_write_log(self._current_ring)
            #self.start_ring_save_log(self._current_ring)

            if custom_root[0] != 0 or custom_root[1] != 0:
                root_location = custom_root
            else:
                self.start_grid_update_stats(n_ring, start)
                root_location = self.__root_location

            ring = Ring(self._current_ring, root_location, self.__radius)
            ring.init_ring_JSON()
            ring.draw_ring()

            #UPDATE RING WITH TYPE OF SEACRH DEEPLY OR MAIN
            generated_ring = ring.get_ring_JSON()
            generated_ring["type_ring"] = self._type_ring
            generated_ring["id_grid"] = self._current_grid
            self.save_ring(generated_ring)

            #self.generated_ring_write_log(generated_ring)
            #self.generated_ring_save_log(generated_ring)
            if custom_root[0] == 0 or custom_root[1] == 0:
                self.generated_ring_update_stats(generated_ring)

            current_ring_values = ring.draw_ring().values()
            consecutive_skipped_hex=0
            foundFalse_hex = False
            self._current_hexagon = 0
            while self._current_hexagon <= len(current_ring_values)-1:

                print "\n"
                print " # >> %s " % str(current_ring_values[self._current_hexagon])
                print " # >> ring %d hexagon %d" % (self._current_ring, self._current_hexagon)

                if self.checkAlsoRequested(current_ring_values[self._current_hexagon]) == False:

                    """
                    Questo permette di ricominciare non dal primo HEx non catturato, ma 
                    dall ultimo catturato
                    """
                    if foundFalse_hex == False and consecutive_skipped_hex>=1:
                        self._current_hexagon = self._current_hexagon - 1
                        consecutive_skipped_hex = 0

                    foundFalse_hex = True
                    self._lastrequest = GoogleReq(
                        self.__coll_nearby_places,
                        self.__db,
                        current_ring_values[self._current_hexagon],
                        'all',
                        (self.__radius), 1,
                        self._sleep,
                        self._list_key[self._used_key]
                    )

                    self._lastrequest.init_nearby_requests_JSON()
                    self._lastrequest.init_nearby_results_JSON()

                    outputRequest = self._lastrequest.loop_nearby_request()

                    if outputRequest == "OK":
                        print "\n # [OK] Request hexagon %d" % self._current_hexagon

                        self._total_hexagon_processed = self._total_hexagon_processed + 1

                        self.save_requests()

                        self.save_nearby_locations()


                        self.finished_request_write_log()
                        #self.finished_request_save_log()

                        self.update_stats(1)

                        #OBBLIGATORIO
                        self.save_finished_gathering()

                        self._current_hexagon = self._current_hexagon + 1

                    elif outputRequest == "TIMEOUT":

                        print "\n # [TIMEOUT] Request "
                        print "# [CHANGE KEY] Request "
                        self.write_log_ERROR("TIMEOUT", generated_ring, self._current_ring, self._current_hexagon)

                        self.change_key(generated_ring)

                        self.update_stats(0)
                        self.write_log_finished_gathering()
                        self.save_finished_gathering()

                    elif outputRequest == "INVALID_REQUEST":

                        print "\n # [INVALID] Request "
                        self.write_log_ERROR("INVALID REQUEST", generated_ring, self._current_ring, self._current_hexagon)

                        self.change_key(generated_ring)

                        self.update_stats(0)
                        self.write_log_finished_gathering()
                        self.save_finished_gathering()

                    elif outputRequest == "OTHER":

                        print "\n # [OTHER EXCEPTION] Request"
                        self.write_log_ERROR("OTHER EXCEPTION", generated_ring, self._current_ring, self._current_hexagon)

                        self.change_key(generated_ring)

                        self.update_stats(0)
                        self.write_log_finished_gathering()
                        self.save_finished_gathering()
                else:
                    self._current_hexagon = self._current_hexagon + 1
                    consecutive_skipped_hex = consecutive_skipped_hex + 1



        self._current_grid = self._current_grid + 1
        self.write_log_finished_gathering()
        self.save_finished_gathering()

        return self.__db_name

    def checkAlsoRequested(self, current_hexagon_latlong):

        out = self.__coll_requests.find({'_latlong':current_hexagon_latlong})
        if out.count() == 0:
            return False
        else:
            return True


    def run_deepply(self, n_ring, start=0):

        #run first execution
        output = self.run(n_ring, start)

        if output == 0:
            return output
        else:

            while (len(self._gathering_stat_JSON["deep_alert_hexagon"]) >= 1):

                self._type_ring = "deep"

                for deep_place_gather in self._gathering_stat_JSON["deep_alert_hexagon"]:

                    print "\n ###  DEEP SEARCH "
                    print "Current new radius: %d" % self.__radius
                    print "Path exagon %s" % deep_place_gather['path_hexagon']
                    print "Original Radius %s" % deep_place_gather['radius']

                    self.__radius = int(math.ceil(float(deep_place_gather['radius'] / 3))) + 1

                    print "New Radius %s" % self.__radius

                    # custom_root=(0,0),type_ring="main"):

                    if self.__radius > self.__min_radius:
                        print "START DEEP >> "
                        self.run(2, 0, deep_place_gather["lat_lon"], self._type_ring)
                        deep_place_gather["gathered"] = 'yes'
                        self._gathering_stat_JSON["deep_alert_hexagon_checked"].append(deep_place_gather)
                        self._gathering_stat_JSON["deep_alert_hexagon"].remove(deep_place_gather)

                        """
                        elif self.__radius << self.__min_radius and \
                                        deep_place_gather["total_place"] >= self._alert_number_places_hexagon:
    
                            #TODO check is
    
                            self.__radius = self.__min_radius - 1
    
                            print "START DEEP at min size >>"
                            self.run(2, 0, deep_place_gather["lat_lon"], self._type_ring)
                            deep_place_gather["gathered"] = 'yes'
                            self._gathering_stat_JSON["deep_alert_hexagon_checked"].append(deep_place_gather)
                            self._gathering_stat_JSON["deep_alert_hexagon"].remove(deep_place_gather)
                        """
                    else:
                        print "SKIPP DEEP XX "
                        print self.__radius
                        print self.__min_radius
                        print deep_place_gather["total_place"]

                        self._gathering_stat_JSON["deep_alert_hexagon"].remove(deep_place_gather)

                        deep_place_gather["skipped_radius"] = self.__radius
                        self._gathering_stat_JSON["deep_alert_hexagon_skipped"].append(deep_place_gather)

                    self.save_finished_gathering()

            return output

    def get_all_place_detail(self):

        var_to_check ='_id'
        output = 0
        cursor_nearby = self.__coll_nearby_places_merged.distinct(var_to_check, allowDiskUse=True)
        cursor_detail_checked = self.__coll_detail_places_checked_merged.distinct(var_to_check, allowDiskUse=True)

        place_detail_checked = dict()

        total_nearby = len(cursor_nearby)

        for doc in range(total_nearby):
            self.write_progression(doc, total_nearby)
            if cursor_nearby[doc] in cursor_detail_checked:
                place_detail_checked = self.__coll_detail_places_checked_merged.find_one({var_to_check: cursor_nearby[doc]})
            else:
                print "> Non esiste in checked"
                nearby_place = self.__coll_nearby_places_merged.find_one({var_to_check:cursor_nearby[doc]})
                place_detail_checked['_id'] = nearby_place['_id']
                place_detail_checked['id'] = nearby_place['id']
                place_detail_checked['place_id'] = nearby_place['place_id']
                place_detail_checked['reference'] = nearby_place['reference']
                place_detail_checked['CHECKED_DETAIL'] = False
                place_detail_checked['CHECKED_TIME'] = datetime.now()

                self.__coll_detail_places_checked_merged.insert_one(place_detail_checked)

            if bool(place_detail_checked['CHECKED_DETAIL']) == False:
                print ">> CHECKED: FALSE"
                output=self.retrive_place_details(place_detail_checked['place_id'], place_detail_checked)
                while output != 'OK' and output != 'INVALID_REQUEST':
                    print '-> ripeto'
                    output = self.retrive_place_details(place_detail_checked['place_id'], place_detail_checked)

        return output

    def retrive_place_details(self, place_reference, place_detail_checked, place_nearby=None):
        place_detail = dict()

        print place_reference

        self._lastrequest = GoogleReq(
            None,
            None,
            None,
            'all',
            None, 1,
            self._sleep,
            self._list_key[self._used_key]
        )
        out = self._lastrequest.makerequest_detail(place_reference)

        if out == "OK":
            place_detail = self._lastrequest.get_last_place_detail()
            self.__coll_detail_places_merged.insert_one(place_detail)
            place_detail_checked['CHECKED_DETAIL'] = True
            place_detail_checked['CHECKED_TIME'] = datetime.now()
            self.__coll_detail_places_checked_merged.find_one_and_replace({'_id': place_detail_checked['_id']}, place_detail_checked)
        elif out == "INVALID_REQUEST":
            print "invalid salto"

            place_detail_checked['USED_KEY'] = self._list_key[self._used_key]
            place_detail = self._lastrequest.get_last_place_detail()
            pprint(place_detail)

            place_detail_checked['CHECKED_DETAIL'] = False
            place_detail_checked['CHECKED_TIME'] = datetime.now()
            place_detail_checked['CHANGED_KEY'] = self._list_key[self._used_key]
            place_detail_checked['ERROR'] = 'INVALID_NOT_FOUND'
            if self.__coll_detail_places_error.find({'_id': place_detail_checked['_id']}).count()>0:
                self.__coll_detail_places_error.find_one_and_replace({'_id': place_detail_checked['_id']}, place_detail_checked)
            else:
                self.__coll_detail_places_error.insert_one({'_id': place_detail_checked['_id']},
                                                                     place_detail_checked)

            self.change_key()
        else:
            print "Changed Key"
            print "Key used %s " % self._list_key[self._used_key]
            print "ID Key used %s " % self._used_key
            place_detail_checked['USED_KEY'] = self._list_key[self._used_key]
            self.change_key()

            place_detail_checked['CHECKED_DETAIL'] = False
            place_detail_checked['CHECKED_TIME'] = datetime.now()
            place_detail_checked['CHANGED_KEY'] = self._list_key[self._used_key]
            self.__coll_detail_places_checked_merged.find_one_and_replace({'_id': place_detail_checked['_id']},
                                                                          place_detail_checked)

        pprint(out)
        return out

    def write_progression(self, n, m):
        sys.stdout.flush()
        sys.stdout.write("\r" + "...")
        sys.stdout.write("\r" + "%d/%d" % (n, m))
        "> Esiste in checked"
        return 1




#def backup():
#    os.system("mongodump --host mongodb://mongo --port 27020 --out /backup")        
#    return 1

def main(argv=None):
    if argv is None:
        argv = sys.argv

    print 'START'

    # city = 'milan'
    # latlng = (45.473154950437475, 9.18726839028014)

    city = 'florence'
    latlng = (43.7789669, 11.2401433)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 0, 2, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(2, 0)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 3, 15, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(15, 3)
    dbname = str(output)
    
    gather = None
    gather = Gathering(latlng, 60, 5, 0, 16, 20, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(20, 16)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 21, 26, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(26, 21)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 27, 32, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(32, 27)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 33, 50, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(50, 33)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 51, 61, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(61, 51)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 62, 73, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(73, 62)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 74, 84, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(84, 74)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 85, 140, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(140, 85)
    dbname = str(output)


    """
    gather = None
    gather = Gathering(latlng, 60, 5, 0, 141, 180, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(180, 141)
    dbname = str(output)

    gather = None
    gather = Gathering(latlng, 60, 5, 0, 181, 200, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(200, 181)
    dbname = str(output)
  
    gather = None
    gather = Gathering(latlng, 60, 5, 0, 201, 230, city)
    gather.init_gathering_stat_JSON()
    output = gather.run_deepply(230, 201)
    dbname = str(output)
    """

    print "FINISH GATHERING ON: %s" % dbname

    #gather.get_all_place_detail()

    return 1
