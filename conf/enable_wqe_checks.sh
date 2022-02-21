#!/bin/bash

mst start

if [[ $# -eq 0 ]]; then
  device=mlx5_0
else
  device=$1
fi

dev=`mst status -v | grep $device | awk '{printf $3; printf "\n"}'`

echo $dev

mcra $dev 0x24908 0x00000000
mcra $dev 0x2490c 0xd03ffffc
mcra $dev 0x24910 0xf5909280
mcra $dev 0x24914 0x4c00267f
mcra $dev 0x24918 0xffffffff
mcra $dev 0x2491c 0xf8563dff

mcra $dev 0x24908,24

mcra $dev 0x24928 0x00000000
mcra $dev 0x2492c 0xd03ffffc
mcra $dev 0x24930 0xf5909280
mcra $dev 0x24934 0x4c00267f
mcra $dev 0x24938 0xffffffff
mcra $dev 0x2493c 0xf8563dff

mcra $dev 0x24928,24
