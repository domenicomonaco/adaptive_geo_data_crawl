 #!/bin/bash

#Adaptive Geolocated-data crawler: intelligent adaptive crawler for Google Place
#Monaco, D. (2019). An adaptive agent for Google Place crawling (No. 934). EasyChair
#3-clause BSD license
#Domenico Monaco, Tecnologie per persone, http://domenicomonaco.it

sudo docker exec -ti mongo sh -c "mongodump --host localhost --port 27020 --out /data/dbdump/out-down"
sudo tar -zcvf data.tar.gz data
