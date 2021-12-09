#!/bin/bash

echo Parallel: one band
echo Parallel: one band >> dask.txt
for i in {1..5}
do
  echo Trial \#$i
  python3 montage-workflow-dask.py --center "56.7 24.0" --degrees 1.0 --band dss:DSS2B:red 2>&1 | grep "Workflow execution" >> dask.txt
done
echo -e "\n" >> dask.txt

rm -rf dask-worker-space

