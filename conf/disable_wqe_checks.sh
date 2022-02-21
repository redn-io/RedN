#!/bin/bash


function zerofy()
{
  local base=$1

  echo "before:"
  mcra $dev $base,24

  a=$base

  i=0
  while [[ $i -lt 6 ]]; do
    mcra $dev 0x`printf '%x\n' $a` 0
    a=$((a+4))
    i=$((i+1))
  done

  echo "after:"
  mcra $dev $base,24
}

if [[ $# -eq 0 ]]; then
  device=mlx5_1
else
  device=$1
fi

dev=`mst status -v | grep $device | awk '{printf $3; printf "\n"}'`

echo "device: $dev"



# for CX-5 and CX-6
zerofy "0x24908"
zerofy "0x24928"



