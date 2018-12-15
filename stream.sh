#!/usr/bin/env bash
# ./stream.py <path_to_file_to_stream> <port> <stream_interval> <batch_size> <ignore_header_flag[true or false]>
sleep 2
ignore_header=$5
while read line; do
    if ${ignore_header} ; then
        ignore_header=false
        continue
    fi
    sleep $3;
    batch_size=$4
	echo "$line"
    while [[ ${batch_size} -gt 1 ]]
    do
        read line
	    echo "$line"
	    batch_size=`expr ${batch_size} - 1`
	done
done < "$1" | nc -lk $2