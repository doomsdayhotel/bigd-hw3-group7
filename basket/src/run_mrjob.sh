#!/bin/bash

python mr_basket.py ../data/*.csv -r hadoop \
       --output-dir basket \
       --python-bin /opt/conda/default/bin/python

