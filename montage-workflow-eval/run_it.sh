#!/bin/bash


echo Sequential >> output.txt
echo Sequential
for i in {1..5}
do
  echo Trial \#$i
  python3 montage-workflow-seq.py --center "56.7 24.0" --degrees 1.0 --band dss:DSS2B:red 2>&1 | grep "Workflow execution" >> output.txt
done
echo -e "\n" >> output.txt


echo Parallel: one band
echo Parallel: one band >> output.txt
for i in {1..5}
do
  echo Trial \#$i
  python3 montage-workflow-dask.py --center "56.7 24.0" --degrees 1.0 --band dss:DSS2B:red 2>&1 | grep "Workflow execution" >> output.txt
done
echo -e "\n" >> output.txt

rm -rf dask-worker-space

