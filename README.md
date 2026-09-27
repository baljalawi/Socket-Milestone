# Socket-Milestone




Team:

\-Bassam Aljalawi





## Four terminals on one computer (local test)

Open four terminals in this folder. On Windows use `python`; on systems where that command is unavailable, use `python3`.

1. Manager: `python manager.py 37500`
2. Peer A: `python peer.py 127.0.0.1 37500`; at its `peer>` prompt: `register Alice 127.0.0.1 37501 37502`
3. Peer B: `python peer.py 127.0.0.1 37500`; then: `register Bob 127.0.0.1 37503 37504`
4. Peer C: `python peer.py 127.0.0.1 37500`; then: `register Carol 127.0.0.1 37505 37506`
5. In Alice's terminal: `setup-dht Alice 3 1950`






