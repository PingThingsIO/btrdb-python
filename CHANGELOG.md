# Changelog

## 5.31.1
### What's Changed
* Sort tables by time by default for any `pyarrow` tables. by @justinGilmer in
* Fix deprecation warnings for pip installations. by @jleifnf in

## 5.31.0

## 5.30.2
### What's Changed
* Update readthedocs to new yaml for testing. by @justinGilmer in https://github.com/PingThingsIO/btrdb-python/pull/40
* Converting pandas index takes very long, add in arrow_table. by @justinGilmer in https://github.com/PingThingsIO/btrdb-python/pull/41


**Full Changelog**: https://github.com/PingThingsIO/btrdb-python/compare/v5.30.1...v5.30.2

## 5.30.1
### What's Changed
* Small version bump for pypi release


**Full Changelog**: https://github.com/PingThingsIO/btrdb-python/compare/v5.30.0...v5.30.1


## 5.30.0
### What's Changed
* Merge Arrow support into Main for Release by @youngale-pingthings in https://github.com/PingThingsIO/btrdb-python/pull/37
  * This PR contains many changes that support the commercial Arrow data fetches and inserts
  * `arrow_` prefixed methods for `Stream` Objects:
    * `insert, aligned_windows, windows, values`
  * `arrow_` prefixed methods for StreamSet` objects:
    * `insert, values, to_dataframe, to_polars, to_arrow_table, to_numpy, to_dict, to_series`
* Justin gilmer patch 1 by @justinGilmer in https://github.com/PingThingsIO/btrdb-python/pull/39


**Full Changelog**: https://github.com/PingThingsIO/btrdb-python/compare/v5.28.1...v5.30.0


## 5.28.1
### What's Changed
* Upgrade ray versions by @jleifnf in https://github.com/PingThingsIO/btrdb-python/pull/15
* Release v5.28.1 and Update Python by @youngale-pingthings in https://github.com/PingThingsIO/btrdb-python/pull/17

### New Contributors
* @jleifnf made their first contribution in https://github.com/PingThingsIO/btrdb-python/pull/15

**Full Changelog**: https://github.com/PingThingsIO/btrdb-python/compare/v5.15.1...v5.28.1
