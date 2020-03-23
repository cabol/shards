# Changelog

## [v0.6.2](https://github.com/cabol/shards/tree/v0.6.2) (2020-03-23)

[Full Changelog](https://github.com/cabol/shards/compare/v0.6.1...v0.6.2)

**Closed issues:**

- Rollback when `insert\_new` fails [\#46](https://github.com/cabol/shards/issues/46)

## [v0.6.1](https://github.com/cabol/shards/tree/v0.6.1) (2019-11-05)

[Full Changelog](https://github.com/cabol/shards/compare/v0.6.0...v0.6.1)

**Closed issues:**

- Missing support for keypos [\#44](https://github.com/cabol/shards/issues/44)
- update\_counter spec is not the same as ETS. [\#42](https://github.com/cabol/shards/issues/42)
- Failed to start shard\_sup [\#41](https://github.com/cabol/shards/issues/41)

## [v0.6.0](https://github.com/cabol/shards/tree/v0.6.0) (2018-09-02)

[Full Changelog](https://github.com/cabol/shards/compare/v0.5.1...v0.6.0)

**Implemented enhancements:**

- Improve documentation [\#39](https://github.com/cabol/shards/issues/39)

## [v0.5.1](https://github.com/cabol/shards/tree/v0.5.1) (2018-06-26)

[Full Changelog](https://github.com/cabol/shards/compare/v0.5.0...v0.5.1)

**Closed issues:**

- Issues on Erlang v21 [\#40](https://github.com/cabol/shards/issues/40)
- Improve test coverage \(ideally 100%\) [\#38](https://github.com/cabol/shards/issues/38)

## [v0.5.0](https://github.com/cabol/shards/tree/v0.5.0) (2017-07-11)

[Full Changelog](https://github.com/cabol/shards/compare/v0.4.2...v0.5.0)

**Implemented enhancements:**

- Improve `shards\_local:info/1,2` to be compliant with ETS functions [\#37](https://github.com/cabol/shards/issues/37)
- Improve `file2tab` and `tab2file` in `shards\_local` to be compliant with ETS functions [\#36](https://github.com/cabol/shards/issues/36)
- Improve `shards\_local` fix \#4 – `ordered\_set` support. [\#25](https://github.com/cabol/shards/issues/25)

**Closed issues:**

- Allows to register `shards\_sup` with custom name \(given as parameter\) [\#34](https://github.com/cabol/shards/issues/34)
- file2tab,tab2file not working  [\#33](https://github.com/cabol/shards/issues/33)

**Merged pull requests:**

- Improve README grammar [\#35](https://github.com/cabol/shards/pull/35) ([casidiablo](https://github.com/casidiablo))

## [v0.4.2](https://github.com/cabol/shards/tree/v0.4.2) (2017-02-22)

[Full Changelog](https://github.com/cabol/shards/compare/v0.4.1...v0.4.2)

**Closed issues:**

- Implement `ets:rename/2` [\#32](https://github.com/cabol/shards/issues/32)
- Fulfil ETS API for `shards\_local` [\#1](https://github.com/cabol/shards/issues/1)

## [v0.4.1](https://github.com/cabol/shards/tree/v0.4.1) (2017-02-13)

[Full Changelog](https://github.com/cabol/shards/compare/v0.4.0...v0.4.1)

**Implemented enhancements:**

- Automatically distributed table setup when `shards:new/2` is called with opt `{nodes, \[node\(\)\]}` [\#28](https://github.com/cabol/shards/issues/28)
- Implement `select/match` pagination ops for `shards\_dist` [\#16](https://github.com/cabol/shards/issues/16)

**Closed issues:**

- Implement `update\_counter` and `update\_element` in `shards\_dist` [\#31](https://github.com/cabol/shards/issues/31)
- LRU Feature may be  [\#29](https://github.com/cabol/shards/issues/29)

## [v0.4.0](https://github.com/cabol/shards/tree/v0.4.0) (2017-02-10)

[Full Changelog](https://github.com/cabol/shards/compare/v0.3.1...v0.4.0)

**Implemented enhancements:**

- Allow to create the `state` passing the rest of its attributes [\#30](https://github.com/cabol/shards/issues/30)

**Closed issues:**

- Add performance and scalability tests [\#2](https://github.com/cabol/shards/issues/2)

## [v0.3.1](https://github.com/cabol/shards/tree/v0.3.1) (2016-09-08)

[Full Changelog](https://github.com/cabol/shards/compare/v0.3.0...v0.3.1)

**Implemented enhancements:**

- Modify `shards:new/2` function to return only the table name in case of success [\#27](https://github.com/cabol/shards/issues/27)

## [v0.3.0](https://github.com/cabol/shards/tree/v0.3.0) (2016-08-02)

[Full Changelog](https://github.com/cabol/shards/compare/v0.2.0...v0.3.0)

**Implemented enhancements:**

- Allow to call `shards\_local` without the state – using a default state. [\#23](https://github.com/cabol/shards/issues/23)
- Unify `pick\_shard\_fun` and `pick\_node\_fun` in a single spec [\#22](https://github.com/cabol/shards/issues/22)
- Separate `shards` from specific consistent hashing implementation. [\#21](https://github.com/cabol/shards/issues/21)

**Closed issues:**

- Remove `auto\_eject\_nodes` property from `state` – it isn't being used [\#24](https://github.com/cabol/shards/issues/24)
- Fix `shards` to work well with `ordered\_set` tables. [\#4](https://github.com/cabol/shards/issues/4)

## [v0.2.0](https://github.com/cabol/shards/tree/v0.2.0) (2016-07-10)

[Full Changelog](https://github.com/cabol/shards/compare/v0.1.0...v0.2.0)

**Implemented enhancements:**

- Modify `shards\_local` to avoid additional table types, handle a flag `sharded` instead. [\#10](https://github.com/cabol/shards/issues/10)
- Make distribution function \(pick none/shard\) configurable. [\#9](https://github.com/cabol/shards/issues/9)
- Implement sharding at global level. [\#3](https://github.com/cabol/shards/issues/3)

**Closed issues:**

- OTP \< 18 not supported [\#13](https://github.com/cabol/shards/issues/13)
- rebar2 compatibility [\#12](https://github.com/cabol/shards/issues/12)

**Merged pull requests:**

- General fixes and refactoring. [\#19](https://github.com/cabol/shards/pull/19) ([cabol](https://github.com/cabol))
- v0.2.0 [\#18](https://github.com/cabol/shards/pull/18) ([cabol](https://github.com/cabol))
- Preparing v0.2.0. [\#17](https://github.com/cabol/shards/pull/17) ([cabol](https://github.com/cabol))
- V0.1.1 [\#15](https://github.com/cabol/shards/pull/15) ([cabol](https://github.com/cabol))
- Enhancements and fix issue \#13. [\#14](https://github.com/cabol/shards/pull/14) ([cabol](https://github.com/cabol))

## [v0.1.0](https://github.com/cabol/shards/tree/v0.1.0) (2016-05-19)

[Full Changelog](https://github.com/cabol/shards/compare/765c5e9f6e350b46076d8a525ac0d18fba909e27...v0.1.0)

**Closed issues:**

- Operation of the shards:info/2 does not match [\#8](https://github.com/cabol/shards/issues/8)

**Merged pull requests:**

- Fix README. [\#7](https://github.com/cabol/shards/pull/7) ([cabol](https://github.com/cabol))
- Refactor shards\_local to handle 'state' and avoid to call ETS control table. [\#6](https://github.com/cabol/shards/pull/6) ([cabol](https://github.com/cabol))
- Implemented distributed shards. [\#5](https://github.com/cabol/shards/pull/5) ([cabol](https://github.com/cabol))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
