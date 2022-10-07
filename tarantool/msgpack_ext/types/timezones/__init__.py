"""
Tarantool timezones module.
"""

from tarantool.msgpack_ext.types.timezones.timezones import (
	TZ_AMBIGUOUS,
	indexToTimezone,
	timezoneToIndex,
	timezoneAbbrevInfo,
)

__all__ = ['TZ_AMBIGUOUS', 'indexToTimezone', 'timezoneToIndex',
           'timezoneAbbrevInfo']
