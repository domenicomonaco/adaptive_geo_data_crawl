#!/usr/bin/env python

#Adaptive Geolocated-data crawler: intelligent adaptive crawler for Google Place
#Monaco, D. (2019). An adaptive agent for Google Place crawling (No. 934). EasyChair
#3-clause BSD license
#Domenico Monaco, Tecnologie per persone, http://domenicomonaco.it

import sys
import geodatacrawl.google
import geodatacrawl.movement
import geodatacrawl.gathering

if __name__ == '__main__':
        listkey= ('keyexample_1_HDKJSHD73',
                  'keyexample_2_HDKJSHD73',
                  'keyexample_3_HDKJSHD73')

        print 'START'

        city = 'NAME_OF_CITY'
        
        ## GEOLOCATION start point
        latlng = (43.7789669, 11.2401433)
        gather = None
        gather = geodatacrawl.gathering.Gathering(latlng, 60, 5, 0, 0, 2, city, listkey)
        gather.init_gathering_stat_JSON()
        output = gather.run_deepply(2, 0)
        dbname = str(output)
        print "FINISH GATHERING ON: %s" % dbname













