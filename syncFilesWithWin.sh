#!/bin/bash

crs_base_dir="/mnt/c/GUBBI/BoxSync/CRS"

cp ~/python/*.py ~/CRS_LNK
cp ~/python/*.properties ~/CRS_LNK
cp ~/python/*.json ~/CRS_LNK
cp ~/python/*.sh ~/CRS_LNK
cp ~/python/how2SetupCronUbuntu.txt ~/CRS_LNK

mkdir -p $crs_base_dir/docker/python $crs_base_dir/docker/startup

cp ~/python/docker/docker* $crs_base_dir/docker
cp ~/python/docker/python/* $crs_base_dir/docker/python
cp ~/python/docker/startup/* $crs_base_dir/docker/startup