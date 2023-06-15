#!/bin/bash

sync_dir="/mnt/c/GUBBI/BoxSync/PYTHON_CNO"

cp ~/python/*.py $sync_dir
cp ~/python/*.properties $sync_dir
cp ~/python/*.json $sync_dir
cp ~/python/*.sh $sync_dir
cp ~/python/how2SetupCronUbuntu.txt $sync_dir

mkdir -p $sync_dir/docker/python $sync_dir/docker/startup

cp ~/python/docker/docker* $sync_dir/docker
cp ~/python/docker/python/* $sync_dir/docker/python
cp ~/python/docker/startup/* $sync_dir/docker/startup