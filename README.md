# Brainer

## Run the Broker

./run_broker --debug

The broker by default runs on `ipc:///tmp/broker.sock`. You can change it by using --broker argument.

## Run a Node

./run_node --node-endpoint ipc:///tmp/node1.sock --replica ipc:///tmp/publisher.sock --debug
