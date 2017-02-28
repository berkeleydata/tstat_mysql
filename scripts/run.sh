#!/bin/bash

if [ $# -eq 1 ]; then
    export dt=$1
else
    #export dt='2016-09-08'
    #export dt='2017-01-10'
    #export dt='2017-01-27'
    export dt='2016-12-26'
fi

table='tstat_analyze_extended'

tsa process -d $dt -r -f flow
#tsa export -t $table