#!/bin/bash

export TASKTOP=$(pwd)
export TASKROOT="${TASKROOT:-${TASKTOP/\/src\/wps.cn\/kingtask/}}"
# TASKTOP sanity check
if [[ "$TASKTOP" == "${TASKTOP/\/src\/wps.cn\/kingtask/}" ]]; then
    echo "WARNING: QINGTOP($TASKTOP) does not contain src/wps.cn/kingtask"
    exit 1   
fi

function add_path()
{
  # $1 path variable
  # $2 path to add
  if [ -d "$2" ] && [[ ":$1:" != *":$2:"* ]]; then
    echo "$1:$2"
  else
    echo "$1"
  fi
}

export GOBIN=$TASKTOP/bin

godep path > /dev/null 2>&1
if [ "$?" = 0 ]; then
    echo "GO=godep go" > build_config.mk
    export GOPATH=`godep path`
#    godep restore
else
    echo "GO=go" > build_config.mk
fi

export GOPATH="$TASKROOT"
