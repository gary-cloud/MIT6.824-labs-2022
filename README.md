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
  ... Passed --   3.1  3   48   11120    0
Test (2A): election after network failure ...
  ... Passed --   8.7  3  135   25592    0
Test (2A): multiple elections ...
  ... Passed --   7.6  7  390   79130    0
PASS
ok      6.824/raft      20.420s
```
### Part B: log ✔
```shell
$ go test -race -run 2B
Test (2B): basic agreement ...
  ... Passed --   0.6  3   22    4858    3
Test (2B): RPC byte count ...
  ... Passed --   1.8  3   54  113254   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   5.7  3   89   20847    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   5.0  5  152   31728    4
Test (2B): concurrent Start()s ...
  ... Passed --   0.7  3   26    6452    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   7.2  3  152   32324    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  23.8  5 3503  999650  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.3  3   62   16561   12
PASS
ok      6.824/raft      48.198s
```
### Part C: persistence ✔
```
$ go test -race -run 2C
Test (2C): basic persistence ...
  ... Passed --   7.1  3  143   29560    7
Test (2C): more persistence ...
  ... Passed --  38.5  5 1338  262482   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.4  3   44    9518    4
Test (2C): Figure 8 ...
  ... Passed --  30.5  5  375   76649   20
Test (2C): unreliable agreement ...
  ... Passed --   5.3  5 1399  417791  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  38.1  5 20353 35416826  636
Test (2C): churn ...
  ... Passed --  16.3  5 2873 1263883  258
Test (2C): unreliable churn ...
  ... Passed --  16.8  5 1208  548457  110
PASS
ok      6.824/raft      155.064s
```
### Part D: log compaction
## Lab 3: Fault-tolerant Key/Value Service
### Part A: Key/value service without snapshots
### Part B: Key/value service with snapshots
## Lab 4: Sharded Key/Value Service
### Part A: The Shard controller
### Part B: Sharded Key/Value Server