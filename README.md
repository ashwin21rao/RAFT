# RAFT

An implementation of the RAFT Distributed Consensus algorithm.

## Starter Code

The code is organized as follows:

```

src/github.com/cmu440/
  raft/                            Raft implementation, tests and test helpers

  rpc/                             RPC library that must be used for implementing Raft

```

## Executing tests

Run the following from the `src/github.com/cmu440/raft/` folder:

```sh
go test -race
```
