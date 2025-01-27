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
$ go test -run 2A
Test (2A): initial election ...
  ... Passed --   3.0  3   60   17100    0
Test (2A): election after network failure ...
  ... Passed --   5.0  3  120   28024    0
Test (2A): multiple elections ...
  ... Passed --   7.4  7  578  130324    0
PASS
ok      6.824/raft      15.441s
```
### Part B: log ✔
```shell
$ go test -run 2B
Test (2B): basic agreement ...
  ... Passed --   0.3  3   18    4846    3
Test (2B): RPC byte count ...
  ... Passed --   3.8  3   49  114081   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   3.3  3   84   22784    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.7  5  178   43398    4
Test (2B): concurrent Start()s ...
  ... Passed --   0.5  3   30    8572    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   8.1  3  207   52039    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  22.9  5 2343 1201005  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.1  3   73   22780   12
PASS
ok      6.824/raft      44.871s
```
### Part C: persistence
### Part D: log compaction
## Lab 3: Fault-tolerant Key/Value Service
### Part A: Key/value service without snapshots
### Part B: Key/value service with snapshots
## Lab 4: Sharded Key/Value Service
### Part A: The Shard controller
### Part B: Sharded Key/Value Server