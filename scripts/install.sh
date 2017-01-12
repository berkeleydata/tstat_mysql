#!/bin/bash

if [ -z $TSA_HOME ]; then
  echo 'Environment variable TSA_HOME is not set!'
  exit -1
fi

cd $TSA_HOME
pip install -r requirements.txt
python setup.py install

#echo "export TSA_HOME=$PWD" >> ~/.bashrc
