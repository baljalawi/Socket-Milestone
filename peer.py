"""CSE 434 milestone DHT peer: UDP manager protocol and clockwise ring setup."""

import argparse
import csv
import ipaddress
import json
import socket
import threading
import uuid
from pathlib import Path


TIMEOUT = 2
RETRIES = 4
PORT_MIN, PORT_MAX = 37500, 37999


def exchange(sock, address, payload):
    """Send a request and match its response; retry with the same request ID."""
    payload = dict(payload, request_id=uuid.uuid4().hex)
    packet = json.dumps(payload, separators=(",", ":")).encode()
    for _ in range(RETRIES):
        sock.sendto(packet, address)
        try:
            while True:
                raw, source = sock.recvfrom(65535)
                if source != address:
                    continue
                answer = json.loads(raw)
                if answer.get("request_id") == payload["request_id"]:
                    return answer
        except socket.timeout:
            pass
    raise TimeoutError(f"No response from {address} for {payload['command']}")


def first_prime_above(value):
    candidate = value + 1
    while True:
        for divisor in range(2, int(candidate ** 0.5) + 1):
            if candidate % divisor == 0:
                break
        else:
            return candidate
        candidate += 1


class Peer:
    def __init__(self, manager):
        self.manager = manager
        self.m_sock = None
        self.p_sock = None
        self.name = None
        self.ip = None
        self.my_id = None
        self.ring = []
        self.table = {}  # hash position -> list of records (collisions preserved)
        self.table_size = None
        self.lock = threading.Lock()
        self.seen = {}

    def manager_request(self, command, **kwargs):
        return exchange(self.m_sock, self.manager, {"command": command, "name": self.name, **kwargs})

    def peer_request(self, destination, command, **kwargs):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(TIMEOUT)
            return exchange(sock, (destination["ip"], destination["p_port"]),
                            {"command": command, **kwargs})

    def right(self):
        return self.ring[(self.my_id + 1) % len(self.ring)]

    def serve(self):
        while True:
            try:
                raw, source = self.p_sock.recvfrom(65535)
                message = json.loads(raw)
                request_id = message["request_id"]
                key = (source, request_id)
                if key in self.seen:
                    response = self.seen[key]
                elif message.get("command") == "set-id":
                    self.my_id = message["id"]
                    self.ring = message["ring"]
                    self.table_size = message["table_size"]
                    self.table.clear()
                    print(f"RECV set-id: ID={self.my_id}, ring size={len(self.ring)}, "
                          f"right={self.right()['name']}", flush=True)
                    response = {"status": "SUCCESS"}
                elif message.get("command") == "store":
                    target, pos = message["target"], message["pos"]
                    if self.my_id is None or target >= len(self.ring) or pos >= self.table_size:
                        response = {"status": "FAILURE", "reason": "invalid ring or position"}
                    elif target == self.my_id:
                        with self.lock:
                            self.table.setdefault(pos, []).append(message["record"])
                        response = {"status": "SUCCESS"}
                    else:
                        print(f"FORWARD store pos={pos} target={target} to {self.right()['name']}", flush=True)
                        response = self.peer_request(self.right(), "store", target=target,
                                                     pos=pos, record=message["record"])
                    self.seen[key] = response
                elif message.get("command") == "count":
                    with self.lock:
                        count = sum(len(bucket) for bucket in self.table.values())
                    response = {"status": "SUCCESS", "count": count}
                    self.seen[key] = response
                else:
                    response = {"status": "FAILURE", "reason": "unknown peer command"}
                response = dict(response, request_id=request_id)
                self.p_sock.sendto(json.dumps(response, separators=(",", ":")).encode(), source)
            except (OSError, ValueError, KeyError, TypeError, TimeoutError) as exc:
                print(f"Peer message error: {exc}", flush=True)

    def register(self, name, ip, m_port, p_port):
        if self.name is not None:
            print("This process is already registered")
            return
        if not (name.isascii() and name.isalpha() and 1 <= len(name) <= 15):
            print("Name must contain 1–15 ASCII letters")
            return
        ipaddress.IPv4Address(ip)
        if not (PORT_MIN <= m_port <= PORT_MAX and PORT_MIN <= p_port <= PORT_MAX and m_port != p_port):
            print(f"Ports must be distinct and in {PORT_MIN}..{PORT_MAX}")
            return
        self.m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.m_sock.bind((ip, m_port))
            self.p_sock.bind((ip, p_port))
            self.m_sock.settimeout(TIMEOUT)
            answer = exchange(self.m_sock, self.manager,
                              {"command": "register", "name": name, "ip": ip,
                               "m_port": m_port, "p_port": p_port})
            print(f"RECV register: {answer['status']} {answer.get('reason', '')}", flush=True)
            if answer["status"] != "SUCCESS":
                self.m_sock.close()
                self.p_sock.close()
                self.m_sock = self.p_sock = None
                return
            self.name, self.ip = name, ip
            threading.Thread(target=self.serve, daemon=True).start()
            print(f"REGISTERED {name}, manager port={m_port}, peer port={p_port}", flush=True)
        except Exception:
            self.m_sock.close()
            self.p_sock.close()
            self.m_sock = self.p_sock = None
            raise

    def setup(self, n, year):
        if not self.name:
            print("Register first")
            return
        # Check the file before locking the manager into its setup state.
        path = Path(__file__).resolve().parent / "data" / f"details-{year}.csv"
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or len(reader.fieldnames) != 14 or "event_id" not in reader.fieldnames:
                raise ValueError("Expected a 14-column storm CSV with an event_id column")
            records = list(reader)
        for record in records:
            int(record["event_id"])
        size = first_prime_above(2 * len(records))
        answer = self.manager_request("setup-dht", n=n, year=year)
        print(f"RECV setup-dht: {answer['status']} {answer.get('reason', '')}", flush=True)
        if answer["status"] != "SUCCESS":
            return
        self.ring, self.my_id, self.table_size = answer["peers"], 0, size
        print(f"Leader ID=0; records={len(records)}; hash table size={size}", flush=True)
        for index, peer in enumerate(self.ring[1:], 1):
            result = self.peer_request(peer, "set-id", id=index,
                                       ring=self.ring, table_size=size)
            if result["status"] != "SUCCESS":
                raise RuntimeError(f"set-id failed at {peer['name']}: {result}")
            print(f"SEND set-id: {peer['name']} ID={index}", flush=True)
        self.table.clear()
        for record in records:
            pos = int(record["event_id"]) % size
            target = pos % n
            if target == 0:
                with self.lock:
                    self.table.setdefault(pos, []).append(record)
            else:
                result = self.peer_request(self.right(), "store", target=target,
                                           pos=pos, record=record)
                if result["status"] != "SUCCESS":
                    raise RuntimeError(f"Store failed for event {record['event_id']}: {result}")
        counts = []
        for index, peer in enumerate(self.ring):
            if index == 0:
                with self.lock:
                    count = sum(len(bucket) for bucket in self.table.values())
            else:
                result = self.peer_request(peer, "count")
                if result["status"] != "SUCCESS":
                    raise RuntimeError(f"Count failed at {peer['name']}")
                count = result["count"]
            counts.append(count)
            print(f"Node {index} ({peer['name']}): {count} records", flush=True)
        if sum(counts) != len(records):
            raise RuntimeError(f"Record count mismatch: {sum(counts)} != {len(records)}")
        result = self.manager_request("dht-complete")
        print(f"SEND dht-complete; RECV {result['status']}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manager_ip")
    parser.add_argument("manager_port", type=int)
    args = parser.parse_args()
    ipaddress.IPv4Address(args.manager_ip)
    if not PORT_MIN <= args.manager_port <= PORT_MAX:
        parser.error(f"manager port must be in {PORT_MIN}..{PORT_MAX}")
    peer = Peer((args.manager_ip, args.manager_port))
    print("Commands: register <name> <IPv4> <m-port> <p-port> | "
          "setup-dht <name> <n> <year> | quit", flush=True)
    while True:
        try:
            parts = input("peer> ").split()
            if not parts:
                continue
            if parts[0] == "quit":
                break
            if parts[0] == "register" and len(parts) == 5:
                peer.register(parts[1], parts[2], int(parts[3]), int(parts[4]))
            elif parts[0] == "setup-dht" and len(parts) == 4:
                if parts[1] != peer.name:
                    print("Only this process's registered name can issue setup-dht")
                else:
                    peer.setup(int(parts[2]), int(parts[3]))
            else:
                print("Unknown command or wrong number of arguments")
        except (OSError, ValueError, TimeoutError, RuntimeError) as exc:
            print(f"ERROR: {exc}", flush=True)
        except (KeyboardInterrupt, EOFError):
            break


if __name__ == "__main__":
    main()
