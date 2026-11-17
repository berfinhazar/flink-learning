#!/usr/bin/env python3
"""Verify Flink window reports with independent decimal batch calculations."""
import csv
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo

root=Path(__file__).resolve().parents[1]
source=Path(sys.argv[1]) if len(sys.argv)>1 else root/'data/FUTURES_TRADES.txt'
output=Path(sys.argv[2]) if len(sys.argv)>2 else root/'output'
zone=ZoneInfo(sys.argv[3] if len(sys.argv)>3 else 'UTC')
symbol=sys.argv[4] if len(sys.argv)>4 else 'XYZ'
threshold=Decimal(sys.argv[5] if len(sys.argv)>5 else '5')
records=[]
with source.open() as f:
    for date,time,price,volume in csv.reader(f):
        timestamp=int(datetime.strptime(date+' '+time,'%d/%m/%Y %H:%M:%S').replace(tzinfo=zone).timestamp())
        records.append((timestamp,Decimal(price),int(volume)))
def iso(seconds):
    return datetime.fromtimestamp(seconds,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
def percent(value,previous):
    if previous==0:return ''
    return str(((Decimal(value)-Decimal(previous))*100/Decimal(previous)).quantize(Decimal('.01'),rounding=ROUND_HALF_UP))
def calculate(seconds):
    groups=defaultdict(list)
    for row in records: groups[row[0]//seconds*seconds].append(row)
    report=[];old=None;old_start=None
    for start,rows in sorted(groups.items()):
        high=max(r[1] for r in rows);low=min(r[1] for r in rows)
        high_volume=max(r[2] for r in rows);low_volume=min(r[2] for r in rows)
        adjacent=old_start is not None and old_start+seconds==start
        common=dict(symbol=symbol,window_start_utc=iso(start),window_end_utc=iso(start+seconds))
        if seconds==60:
            report.append(dict(common,max_price=str(high),min_price=str(low),max_volume=str(high_volume),min_volume=str(low_volume),
                price_change_pct='0.00' if old is None else percent(high,old[0]) if adjacent else '',
                volume_change_pct='0.00' if old is None else percent(high_volume,old[1]) if adjacent else '',record_count=str(len(rows))))
        elif adjacent and abs(high-old[0])*100>old[0]*threshold:
            report.append(dict(common,previous_max_price=str(old[0]),current_max_price=str(high),price_change_pct=percent(high,old[0]),
                max_price_time_utc=iso(min(r[0] for r in rows if r[1]==high))))
        old=(high,high_volume);old_start=start
    return report
numeric={'max_price','min_price','max_volume','min_volume','record_count','previous_max_price','current_max_price','price_change_pct','volume_change_pct'}
def normalized(row):
    return {k:Decimal(v) if k in numeric and v!='' else v for k,v in row.items()}
for filename,seconds in [('minute-report.csv',60),('price-alerts.csv',300)]:
    with (output/filename).open() as f:actual=sorted(csv.DictReader(f),key=lambda r:r['window_start_utc'])
    expected=calculate(seconds)
    assert len(actual)==len(expected),(filename,len(actual),len(expected))
    for a,b in zip(actual,expected):assert normalized(a)==normalized(b),(filename,a,b)
    print(f'PASS: {filename}: {len(actual)} rows; all fields match.')
print(f'PASS: {len(records)} source records checked.')
if source.name=='demo-trades.txt':
    alerts=calculate(300)
    assert not any(r['window_start_utc'].endswith('00:05:00Z') for r in alerts)
    assert any(Decimal(r['price_change_pct'])>0 for r in alerts)
    assert any(Decimal(r['price_change_pct'])<0 for r in alerts)
    assert not any(r['window_start_utc'].endswith('00:25:00Z') for r in alerts)
    assert calculate(60)[-1]['volume_change_pct']==''
    print('PASS: exact 5% boundary, increases/decreases, gaps, zero volume, single-row and final windows.')
