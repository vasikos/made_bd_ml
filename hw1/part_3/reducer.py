#!/usr/bin/env python3
"""reducer.py"""

import sys

price_cnt = 0
price_sum = 0
price2_sum = 0

for line in sys.stdin:
    cnt_i, sum_i, sum2_i = [float(x) for x in line.split()]
    price_cnt += cnt_i
    price_sum += sum_i
    price2_sum += sum2_i

print(price_sum / price_cnt)
print((price2_sum - price_sum * price_sum / price_cnt) / price_cnt)
    
    