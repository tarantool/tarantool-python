"""
Script to validate that each Tarantool timezone is either a valid pytz
timezone or an addreviated timezone with explicit offset provided.
"""

import pytz
from timezones import timezoneToIndex, timezoneAbbrevInfo

if __name__ != '__main__':
    raise Error('Import not expected')

for timezone in timezoneToIndex.keys():
    if timezone in pytz.all_timezones:
        continue

    if not timezone in timezoneAbbrevInfo:
        raise Exception(f'Unknown Tarantool timezone {timezone}')
