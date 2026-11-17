#!/usr/bin/env python3
"""Compare every Flink alert against an independent batch reference calculation."""
import csv
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

root=Path(__file__).resolve().parents[1]
data=Path(sys.argv[1]) if len(sys.argv)>1 else root/'data/course'
output=Path(sys.argv[2]) if len(sys.argv)>2 else root/'output/course-alerts.csv'
zone=ZoneInfo(sys.argv[3] if len(sys.argv)>3 else 'Asia/Kolkata')
def read(name):
    with (data/name).open() as f:
        return [r for r in csv.reader(f) if r]
customers={r[0].strip() for r in read('alarmed_cust.txt')}
cards={r[0].strip() for r in read('lost_cards.txt')}
transactions=[]
for i,r in enumerate(read('bank_data.txt'),1):
    t=datetime.strptime(r[1],'%Y-%m-%d %H:%M:%S').replace(tzinfo=zone)
    transactions.append((t,i,r))
transactions.sort(key=lambda r:(r[0],r[1]))
expected=Counter()
for position,(t,i,r) in enumerate(transactions):
    active=[x for x in transactions[:position+1] if x[2][3]==r[3] and t-timedelta(seconds=60)<x[0]<=t]
    cities=len({x[2][2].lower() for x in active})
    reasons=[]
    if r[3] in customers:reasons.append('ALARMED_CUSTOMER')
    if r[5] in cards:reasons.append('LOST_CARD')
    if len(active)>10:reasons.append('MORE_THAN_10_TRANSACTIONS_60S')
    if cities>2:reasons.append('MORE_THAN_2_CITIES_60S')
    for reason in reasons:
        expected[(str(i),r[0],t.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),r[3],'****'+r[5][-4:],reason,str(len(active)),str(cities))]+=1
with output.open() as f:
    rows=list(csv.DictReader(f))
actual=Counter(tuple(r[k] for k in ['input_row','transaction_id','time_utc','customer_id','card_masked','reason','transactions_60s','cities_60s']) for r in rows)
assert actual==expected, f'Missing: {expected-actual}; unexpected: {actual-expected}'
print(f'PASS: {len(transactions)} input rows; {len(rows)} alerts exactly match reference.')
print(dict(Counter(r['reason'] for r in rows)))
if data.name=='demo':
    assert set(r['reason'] for r in rows)=={'ALARMED_CUSTOMER','LOST_CARD','MORE_THAN_10_TRANSACTIONS_60S','MORE_THAN_2_CITIES_60S'}
    assert not any(r['transaction_id'] in {'CITY20','RESET','BOUNDARY_B'} for r in rows)
    assert sum(r['transaction_id']=='FLAG' for r in rows)==2
    print('PASS: all four rules, multiple reasons, two-city revisit, expiry, and 60-second boundary.')
