#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

price_sum = 0
price2_sum = 0
price_cnt = 0
for line in csv.reader(sys.stdin):
    if len(line) < 10:
        continue
    
    price_raw = line[9]

    try:
        price = int(price_raw)
        price_sum += price
        price2_sum += price * price
        price_cnt += 1
    except:
        pass

print(price_cnt, price_sum, price2_sum)
