#!/bin/bash
for i in {1..20}
do
    go test -race -run 2A
done