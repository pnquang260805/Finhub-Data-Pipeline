#!/bin/bash
echo "$(date): Start calling quote data"
/usr/local/bin/python3 /call_data/main.py
echo "$(date): Done"