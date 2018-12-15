#!/usr/bin/env bash
# ./stream.py <path_to_file_to_stream> <port> <stream_interval> <batch_size> <ignore_header_flag[true or false]>
sleep 2
ignore_header=$5
while read line; do
    if ${ignore_header} ; then
        ignore_header=false
        continue
    fi
    batch_size=$4
	echo "$line"
    while [[ ${batch_size} -gt 1 ]]
    do
        if read line; then
	        echo "$line"
	    fi
	    batch_size=`expr ${batch_size} - 1`
	done
	sleep $3;
done < "$1" | nc -lk $2