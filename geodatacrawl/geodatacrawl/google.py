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

class GoogleReq:

    def __init__(self, coll, db, latlong, type, radius, loopnext=0, timesleep=5, key=None):

        self._results_JSON = list()
        self._requests_JSON = dict()
        self._var = None
        self._collection = coll
        self._db = db
        self._latlong = latlong
        self._type = type
        self._radius = radius
        self._loopnext = loopnext
        self._timesleep = timesleep

        self._my_key = key

        self._status_ok = 'OK'
        self._status_zero = 'ZERO_RESULTS'
        self._status_over_li = 'OVER_QUERY_LIMIT'
        self._status_denied = 'REQUEST_DENIED'
        self. _status_invalid = 'INVALID_REQUEST'

        self._key_nextpage = 'next_page_token'

        self._GCLIENT = None

        self._last_place_detail = dict()

    def init_nearby_requests_JSON(self):
        self._requests_JSON["_latlong"] = self._latlong
        self._requests_JSON["_type"] = self._type
        self._requests_JSON["_enable_loopnext"] = self._loopnext
        self._requests_JSON["_timesleep"] = self._timesleep
        self._requests_JSON["_start_time"] = datetime.now()

        self._requests_JSON["_end_time"] = ""
        self._requests_JSON["_total_places"] = 0
        self._requests_JSON["_total_pages"] = 0
        self._requests_JSON["_requests_pages"] = list()

        return 1

    def get_nearby_requests_JSON(self):
        return self._requests_JSON

    def init_nearby_results_JSON(self):
        self._results_JSON = list()

    def get_nearby_results_JSON(self):
        return self._results_JSON

    def get_last_place_detail(self):
        return self._last_place_detail

    def makerequest_detail(self, reference=None):
        time.sleep(self._timesleep)
        try:
            _gclient = googlemaps.Client(key=self._my_key)

            self._requests_JSON["_used_key"] = self._my_key
            self._requests_JSON["_radius"] = self._radius

            self._last_place_detail = _gclient.place(reference)

            return "OK"
        except googlemaps.exceptions.Timeout:
            return "TIMEOUT"
        except googlemaps.exceptions.ApiError:
            return "INVALID_REQUEST"
        except:
            return "OTHER"
        return 0

    def makerequest(self, page_token=None):

        try:
            _gclient = googlemaps.Client(key=self._my_key)

            self._requests_JSON["_used_key"] = self._my_key
            self._requests_JSON["_radius"] = self._radius

            self._var = _gclient.places_nearby(self._latlong, self._radius, page_token=page_token)

            return "OK"
        except googlemaps.exceptions.Timeout:
            return "TIMEOUT"
        except googlemaps.exceptions.ApiError:
            return "INVALID_REQUEST"
        except:
            return "OTHER"
        return 0

    def checknextpage(self, response):
        if self._key_nextpage in response.keys():
            return 1
        else:
            return 0

    def write_log_request(self):
        print "\n" \
              "# Start Request:" \
              "\n" \
              "## location: %s" \
              "\n" \
              "## _start_time: %s" \
              "\n" \
              "## current_page: %s" \
                % (
                    str(self._requests_JSON["_latlong"]),
                    str(self._requests_JSON["_start_time"]),
                    str(self._requests_JSON["_total_pages"])
                 )
        return 1

    def loop_nearby_request(self, nextpage=None):

        self.write_log_request()

        time.sleep(self._timesleep)

        if nextpage == None:
            out = self.makerequest()
        else:
            out = self.makerequest(nextpage)

        if out == "OK":

            self._requests_JSON["_requests_pages"].append(self._var)

            self._requests_JSON["_total_pages"] = self._requests_JSON["_total_pages"] + 1
            self._requests_JSON["_total_places"] = self._requests_JSON["_total_places"] + len(self._var['results'])

            for item in self._var['results']:
                self._results_JSON.append(item)


            if self._loopnext:
                if self.checknextpage(self._var):
                    self.loop_nearby_request(self._var['next_page_token'])

            self._requests_JSON["_end_time"] = datetime.now()

            return "OK"

            ##self._requests_JSON["_status_request"] = "TIMEOUT"
            ##self._requests_JSON["_end_time"] = datetime.now()

        elif out == "TIMEOUT":
            return "TIMEOUT"
        elif out == "INVALID_REQUEST":
            return "INVALID_REQUEST"
        elif out == "OTHER":
            return "OTHER"
