#!/bin/bash

function get_available {
    echo `free -g|awk 'NR==2 {print $7}'`
}

function set {
    kb=`gb_to_kb $1`
    modprobe brd rd_nr=1 rd_size=${kb}
    dd if=/dev/zero of=/dev/ram0 bs=1k count=${kb}
}

function clear {
    rmmod brd
}

function gb_to_kb {
    echo `expr $1 \* 1024 \* 1024`
}

# if [ ! $# -eq 1 ]
# then
#     echo "$0 <target memory in GB>"
#     exit 1
# fi

if [ -z "$PDP_server_memory_gb" ]
then
    PDP_server_memory_gb=0
    if [ $# -eq 1 ]
    then
        PDP_server_memory_gb=$1
    fi
fi

echo target memory: ${PDP_server_memory_gb}GB
target=${PDP_server_memory_gb}

if [ $target -eq 0 ]
then
    clear
    exit 0
fi

if [ `get_available` -eq $target ]
then
    echo already done.
    exit 0
fi

clear

avail=`get_available`
if [ $avail -lt $target ]
then
    echo "target ${target}GB > available ${avail}GB"
    exit 1
fi

set `expr $avail - $target`
