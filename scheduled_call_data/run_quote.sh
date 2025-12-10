#!/bin/bash
echo "$(date): Start calling quote data"
/usr/local/bin/python3 /call_data/quote.py
echo "$(date): Done"