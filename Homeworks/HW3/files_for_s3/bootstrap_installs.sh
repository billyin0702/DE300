#!/bin/bash

# Correctly upgrade pip
python3 -m pip install --upgrade pip

# Install packages
sudo python3 -m pip install boto3 pandas "s3fs<=0.4" numpy requests scrapy