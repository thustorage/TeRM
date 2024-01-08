#!/bin/bash

running=0
pattern="'tmp/memcached|scripts/bootstrap.py|scripts/bootstrap-octopus.py|scripts/bootstrap-xstore.py|figure|octopus|xstore|mpibw|dmfs|fserver|master|ycsb|perf'"
check_cmd="pgrep -fa $pattern"

echo "--- node166 ---"
ret=`ssh node166 $check_cmd`
ssh node166 $check_cmd
[ -z "$ret" ] || running=1
echo

echo "--- node168 ---"
ret=`ssh node168 $check_cmd`
ssh node168 $check_cmd
[ -z "$ret" ] || running=1
echo

echo "--- node184 ---"
ret=`ssh node184 $check_cmd`
ssh node184 $check_cmd
[ -z "$ret" ] || running=1
echo

echo "--- node181 ---"
ret=`bash -c "$check_cmd"`
bash -c "$check_cmd"
[ -z "$ret" ] || running=1
echo

[ $running -eq 0 ] && echo "not found" && exit

read -p "Confirm to kill? (Y/N): " confirm && [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]] || exit

kill_cmd="pkill -f $pattern"
echo "--- node166 ---"
ssh node166 $kill_cmd
echo "--- node168 ---"
ssh node168 $kill_cmd
echo "--- node184 ---"
ssh node184 $kill_cmd
echo "--- node181 ---"
bash -c "$kill_cmd"

echo "killed"
