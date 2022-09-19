import pytz
from timezones import timezoneToIndex, timezoneAbbrevInfo

if __name__ != '__main__':
    raise Error('Import not expected')

for timezone in timezoneToIndex.keys():
    if timezone in pytz.all_timezones:
        continue

    if not timezone in timezoneAbbrevInfo:
        raise Exception(f'Unknown Tarantool timezone {timezone}')
