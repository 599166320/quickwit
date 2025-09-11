#!/bin/bash
set -e
rm -r -f ./tail-sampling
rm -r -f /Users/hk00518ml/rust-project/tail-sampling/target/*
cp -r /Users/hk00518ml/rust-project/tail-sampling ./
docker build --build-arg CARGO_FEATURES=quickwit-indexing/kafka,quickwit-indexing/kinesis,quickwit-indexing/pulsar,quickwit-indexing/gcp-pubsub,quickwit-indexing/tail-sampling-kafka -t my-quickwit-image .
#docker run --platform linux/amd64 --rm -it -v "$(pwd)":/quickwit -v "$(pwd)/tail-sampling":/Users/hk00518ml/rust-project/tail-sampling  rust:1.81-bookworm  bash /quickwit/build.sh
rm -r -f ./tail-sampling