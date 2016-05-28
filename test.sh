#!/usr/bin/env bash

DRIVER="ndd_slurm.py"
NDD_DIR="$HOME/ndd/"
TEST_DIR=$NDD_DIR"test/"
PORT=3687

available=()
unreachable=()

check_hosts() {
  for host in $@; do
    ping -c 1 $host > /dev/null
    if [ $? -eq 0 ]; then
      available+=($host)
    else
      unreachable+=($host)
    fi
  done
  if [ ${#unreachable[@]} -ne 0 ]; then
    echo Unreachable hosts: ${unreachable[@]}
  fi
}

prepare_hosts() {
  for host in ${available[@]}; do
    if ! ssh $host [ -d $NDD_DIR ]; then
      ssh $host mkdir $NDD_DIR
    fi
    if ! ssh $host [ -d $TEST_DIR ]; then
      ssh $host mkdir $TEST_DIR
    fi
  done
}

send_driver() {
  for host in ${available[@]}; do
    scp $DRIVER $host:$NDD_DIR$DRIVER
  done
}

get_slaves() {
  slaves=()
  hosts=$@
  for host in ${hosts[@]}; do
    slaves+=("-d "$host)
  done
  echo ${slaves[@]}
}

run() {
  ip=$(/sbin/ifconfig | grep 172 | awk '{print $2}' | cut -d ':' -f2)
  slaves=$(get_slaves $available)
  python ndd_slurm.py -s $ip -i $TEST_DIR -o $TEST_DIR ${slaves[@]}\
                      -p $PORT -H -r
}

main() {
  check_hosts $@
  prepare_hosts
  send_driver
  run
}

main $@
