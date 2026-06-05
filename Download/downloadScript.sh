#!/bin/bash

for i in $(seq 2015 2025);
do
    #python ./Download_test10.py --year $i --all-quarters
    seq 2015 2025 | xargs -P 5 -I {} python ./Download_test10.py --year {} --all-quarters
done
