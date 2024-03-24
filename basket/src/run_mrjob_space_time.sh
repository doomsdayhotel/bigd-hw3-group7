#!/bin/bash

python mr_basket_space_time.py ../data/*.csv -r hadoop \
       --output-dir basket \
       --python-bin /opt/conda/default/bin/python

