#!/bin/bash

echo Sequential > seq.txt
echo Sequential
for i in {1..5}
do
  echo Trial \#$i
  python3 montage-workflow-seq.py --center "56.7 24.0" --degrees 1.0 --band dss:DSS2B:red --verbose
done
echo -e "\n" >> seq.txt


