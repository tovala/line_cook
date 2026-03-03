#!/usr/bin/env python3

# Note: to make this file executable, run chmod +x {filepath}

import csv
import json
import gzip
import sys

input_file=sys.argv[1]
output_file=sys.argv[2]

# Open pipe-delimited file and gzipped output file
with open(input_file, 'r', encoding='utf-8') as f_in, \
     gzip.open(output_file, 'wt', encoding='utf-8') as f_out:
    
    # Use DictReader to automatically handle headers
    reader = csv.DictReader(f_in, delimiter='|')
    
    for row in reader:
        # Write each record as a JSON line
        f_out.write(json.dumps(row) + '\n')