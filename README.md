# MIT 6.824 Labs - Gary
## Lab 1: MapReduce ✔
```shell
$ bash test-mr.sh
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```
## Lab 2: Raft
### Part A: leader election ✔
```shell
$ go test -race -run 2A
Test (2A): initial election ...
  ... Passed --   3.0  3   66   16644    0
Test (2A): election after network failure ...
  ... Passed --   7.5  3   33    7317    0
Test (2A): multiple elections ...
  ... Passed --   5.5  7  190   44960    0
PASS
ok      6.824/raft      17.072s
```
### Part B: log ✔
```shell
$ go test -race -run 2B
Test (2B): basic agreement ...
  ... Passed --   0.4  3   19    4826    3
Test (2B): RPC byte count ...
  ... Passed --   1.3  3   54  113992   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   7.3  3  117   31323    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   7.3  5  199   48885    2
Test (2B): concurrent Start()s ...
  ... Passed --   0.6  3   26    7166    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   8.0  3  144   35939    6
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  25.0  5 1263 1004788  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.2  3   70   20656   12
PASS
ok      6.824/raft      53.243
```
### Part C: persistence
### Part D: log compaction
## Lab 3: Fault-tolerant Key/Value Service
### Part A: Key/value service without snapshots
### Part B: Key/value service with snapshots
## Lab 4: Sharded Key/Value Service
### Part A: The Shard controller
### Part B: Sharded Key/Value Server