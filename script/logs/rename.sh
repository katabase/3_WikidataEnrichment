#!/bin/bash

cwd=$(pwd) # logs/

rgx="^queried(_.\.json)$"  # regex to match queried_*.json

for f in *; do
  if [[ $f =~ $rgx ]]; then
    key="${BASH_REMATCH[1]}"
    new="idqueried$key"
    mv $f $new
  fi
done
