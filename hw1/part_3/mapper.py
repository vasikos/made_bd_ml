#!/usr/bin/env python
"""mapper.py"""

import sys
import csv

price_sum = 0
price2_sum = 0
price_cnt = 0
for line in csv.reader(sys.stdin):
    # line = line.strip('\n')
    # print(list(csv.reader([line.strip()])))
    # splitted = list(csv.reader([line.strip()]))
    # print(line)
#     if len(splitted) < 1:
#         print(line + 'bad_split')
#         continue
#     # print(splitted[0], print(type(splitted[0])))
    if len(line) < 10:
        # print(line + 'bad_len' + str(len(splitted[0])))
        continue
    
    price_raw = line[9]
#     # print(price_raw)

    try:
        price = int(price_raw)
        price_sum += price
        price2_sum += price * price
        price_cnt += 1
    except:
        # print(line + 'bad_price_' + price_raw)
        pass

print(price_cnt, price_sum, price2_sum)
