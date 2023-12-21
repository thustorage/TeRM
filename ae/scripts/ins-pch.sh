#!/bin/bash
cd /home/yz/workspace/TeRM/libterm/kmod
make
rmmod pch
insmod pch.ko
