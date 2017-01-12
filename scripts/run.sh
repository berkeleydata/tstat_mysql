#!/bin/bash

if [ $# -eq 1 ]; then
    export dt=$1
else
    export dt='2016-07-21'
fi

table='tstat_analyze_extended'

tsa process -d $dt
tsa export -t $table