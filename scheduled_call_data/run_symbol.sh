#!/bin/bash
echo "$(date): Start calling symbol data"
/usr/local/bin/python3 /call_data/symbol.py
echo "$(date): Done"