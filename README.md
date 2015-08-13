# Brainer

Brainer is a simple distributed caching mechanism implemented under
the constraint of 48 hours.

It uses Twisted and ZeroMQ. The client is synchronous.

## How it Works (or really: Trade-Offs)

It has a broker-based approach. Obviously, this is a single point of failure. This architecture was chosen due to my lack of experience with ZeroMQ and the 48 hours constraint but it's certainly not ideal. The good news is that with the current code it should not be hard to make it work in a more ring-based approach.

To solve both the resiliency (losing a node without losing data) and the no downtime problem (adding or removing nodes with no downtime), writes are costly.

When writing, all nodes get written. The client can tune this behaviour, though.

The default write behaviour writes to all nodes before returning to the client. You can disable that and make the broker return as soon as the main node has been writen (main in the sense of right node picked by the hashing scheme).

That means that when a node goes down, all other nodes will have that data anyway, so they'll just implicitely assume it when the hashing now starts returning a different machine number.

When a node goes up, the broker requests a snapshot of the cache from any other node and returns that back to the node, which then replay it.

I've aimed for best engineering practices. So a lot of things are easily achieved in the future. A good example is the Cache itself. You can easily code a custom behaviour storage that writes to disk every N writes and fire up a node with it.

But there are still things that can be decoupled around the code. One of them is the node management itself (registering, unregistering, etc) that is in the Broker class.

Finally, this is by no means, a robust implementation. There are no retries on failed writes, for example.

But honestly, given the timeframe, I'm fairly happy with it.

## Install (optional)

```
python setup.py install
```

run_broker, run_node and brainer-cli will be available as binaries in the path after that. But that is optional, you can simply run them from inside the source folder as well.

## Run the Broker

./run_broker --debug

The broker by default runs on `ipc:///tmp/broker.sock`. You can change it by using --broker argument.

## Run a Node

./run_node --node-endpoint ipc:///tmp/node1.sock --debug

You can use the --broker argument to set where the broker is located. run_node assumes the default `ipc:///tmp/broker.sock`

## Operations

You can use a (very) simple console client.

./brainer-cli ipc:///tmp/broker.sock set mykey myvalue
./brainer-cli ipc:///tmp/broker.sock get mykey

Or you can use a (very) simple client library.

```python
from brainer.client import Brainer

client = Brainer('ipc:///tmp/broker.sock')
client.connect()
cient.set("mykey", "myvalue")
client.get("mykey")
```
