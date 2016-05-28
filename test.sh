#!/usr/bin/env bash

DRIVER="ndd_slurm.py"
NDD_DIR="$HOME/ndd/"
TEST_DIR=$NDD_DIR"test/"
PORT=3687

hosts=()

prepare_hosts() {
  for host in ${hosts[@]}; do
    if ! ssh "$host" [ -d "$NDD_DIR" ]; then
      ssh "$host" mkdir "$NDD_DIR"
    fi
    if ! ssh "$host" [ -d "$TEST_DIR" ]; then
      ssh "$host" mkdir "$TEST_DIR"
    fi
  done
}

send_driver() {
  for host in ${hosts[@]}; do
    scp "$DRIVER" "$host:$NDD_DIR$DRIVER"
  done
}

get_slaves() {
  slaves=()
  for host in ${hosts[@]}; do
    slaves+=("-d $host")
  done
  echo ${slaves[@]}
}

run() {
  slaves=$(get_slaves)
  python ndd_slurm.py -s "$MASTER_IP" -i "$TEST_DIR" -o "$TEST_DIR"\
                      ${slaves[@]} -p "$PORT" -H -r
}

main() {
  while [[ $# > 1 ]]; do
    key=$1
    case $key in
      -i)
        MASTER_IP="$2"
        shift
        ;;
      -d)
        hosts+=($2)
        shift
        ;;
      *)
        usage "$0"
        ;;
    esac
    shift
  done
  prepare_hosts
  send_driver
  run
}

main $@
