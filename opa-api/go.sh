#!/bin/sh

ags-download -u ags-download -u http://gis.phila.gov/ArcGIS/rest/services/PhilaGov/Addresses/MapServer/0 -f . -n -o BRT_ID

node toList



