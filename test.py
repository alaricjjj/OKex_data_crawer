import time,datetime

def transfer_timestamp_to_datetime(timestamp):
    datetime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(timestamp))
    return datetime

def transfer_datetime_to_timestamp(datetime):
    now=time.strptime(datetime, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(now))
    return timestamp

def datetime2timestamp(datetime_str):
    d = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    t = d.timetuple()
    timestamp = int(time.mktime(t))
    return timestamp

def timestamp2datetime(timestamp):
    datetime_str = str(datetime.fromtimestamp(timestamp))
    datetime_str = datetime_str.replace(' ', 'T') + '.000Z'
    return datetime_str

def iso2datetime(iso):
    iso_str = iso.replace('T', ' ')
    iso_str = iso_str[:19]
    return iso_str

def datetime2iso(datetime):
    aa = transfer_datetime_to_timestamp(datetime)
    bb = timestamp2datetime(aa)
    return bb

'''把datetime转换成iso'''
# aa = datetime2iso('2019-03-19 16:00:00')
# print(aa)
#
# '''把iso转换成datetime'''
# cc = iso2datetime('2019-03-19T16:00:00.000Z')
# print(cc)

from_time = '2021-03-01 00:00:00'
to_time =   '2021-03-05 08:00:00'

# from datetime import timedelta

def datetime_plus_eighthour(pre_dt):
    aft_dt = datetime.datetime.strptime(pre_dt,"%Y-%m-%d %H:%M:%S")
    t1 = aft_dt + datetime.timedelta(hours=8)
    return t1

print(datetime_plus_eighthour(pre_dt='2021-03-01 00:00:00'))