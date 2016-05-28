#!/bin/bash


DRIVER="ndd_slurm.py"
NDD_DIR="$HOME/ndd/"
TEST_DIR=$NDD_DIR"test/"
PORT=3687


failed=()
available=()


function blacklist {
  echo Error: no $NDD_DIR on $1
  failed+=($1)
  return
}


function whitelist {
  available+=($1)
  return
}


function check_hosts {
  for host in $@; do
    ping -c 1 $host > /dev/null
    if [ $? -ne 0 ]; then
      blacklist $host
    else
      whitelist $host
    fi
  done
  if [ ${#failed[@]} -ne 0 ]; then
    echo Failed hosts are: ${failed[@]}
  fi
  return
}


function prepare_hosts {
  for host in ${available[@]}; do
    if ! ssh $host [ -d $NDD_DIR ]; then
      ssh $host mkdir $NDD_DIR
    fi
    if ! ssh $host [ -d $TEST_DIR ]; then
      ssh $host mkdir $TEST_DIR
    fi
  done
  return
}


function send_driver {
  for host in ${available[@]}; do
    scp $DRIVER $host:$NDD_DIR$DRIVER
  done
  return
}


function get_spec {
  spec=()
  hosts=$@
  for host in ${hosts[@]}; do
    spec+=("-d "$host)
  done
  echo ${spec[@]}
}


function run {
  ip=$(/sbin/ifconfig | grep 172 | awk '{print $2}' | cut -d ':' -f2)
  spec=$(get_spec $available)
  python ndd_slurm.py -s $ip -i $TEST_DIR -o $TEST_DIR ${spec[@]}\
                      -p $PORT -H -r
}


function main {
  check_hosts $@
  prepare_hosts
  send_driver
  run
  return
}

main $@
