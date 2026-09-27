"""CSE 434 milestone DHT manager. UDP JSON messages, one JSON object per datagram."""

import argparse
import ipaddress
import json
import random
import socket

PORT_MIN, PORT_MAX = 37500, 37999


def reply(sock, address, message):
    sock.sendto(json.dumps(message, separators=(",", ":")).encode(), address)


def valid_port(value):
    return isinstance(value, int) and not isinstance(value, bool) and PORT_MIN <= value <= PORT_MAX


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("port", type=int, help="manager listening UDP port")
    args = parser.parse_args()
    if not valid_port(args.port):
        parser.error(f"port must be in {PORT_MIN}..{PORT_MAX}")

    peers = {}  # name -> {name, ip, m_port, p_port, state}
    dht = None
    waiting = False
    cache = {}  # (source IP, source port, request id) -> response
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("0.0.0.0", args.port))
        print(f"Manager listening on UDP {args.port}", flush=True)
        while True:
            data, addr = sock.recvfrom(65535)
            try:
                msg = json.loads(data)
                if not isinstance(msg, dict):
                    continue
                request_id = msg.get("request_id")
                if not isinstance(request_id, str):
                    continue
                key = (addr[0], addr[1], request_id)
                if key in cache:
                    reply(sock, addr, cache[key])
                    continue
                command = msg.get("command")
                name = msg.get("name")
                print(f"RECV {command} {name} from {addr}", flush=True)
                result = {"status": "FAILURE", "reason": "invalid command or parameters"}
                sender = peers.get(name)
                authorized = sender is not None and addr == (sender["ip"], sender["m_port"])

                if waiting and command != "dht-complete":
                    result["reason"] = "waiting for dht-complete"
                elif command == "register":
                    ip = msg.get("ip")
                    m_port = msg.get("m_port")
                    p_port = msg.get("p_port")
                    try:
                        ipaddress.IPv4Address(ip)
                        good_ip = True
                    except (ValueError, TypeError):
                        good_ip = False
                    used = {args.port}
                    for peer in peers.values():
                        used.update((peer["m_port"], peer["p_port"]))
                    if (isinstance(name, str) and name.isascii() and name.isalpha()
                            and len(name) <= 15 and name not in peers and good_ip
                            and addr == (ip, m_port) and valid_port(m_port)
                            and valid_port(p_port) and m_port != p_port
                            and m_port not in used and p_port not in used):
                        peers[name] = {"name": name, "ip": ip, "m_port": m_port,
                                       "p_port": p_port, "state": "Free"}
                        result = {"status": "SUCCESS"}
                        print(f"REGISTERED {name} at {ip} m={m_port} p={p_port}", flush=True)
                elif command == "setup-dht":
                    n, year = msg.get("n"), msg.get("year")
                    available = [p for p in peers.values() if p["state"] == "Free" and p["name"] != name]
                    if (authorized and sender["state"] == "Free" and dht is None
                            and isinstance(n, int) and not isinstance(n, bool) and n >= 3
                            and isinstance(year, int) and not isinstance(year, bool)
                            and 1950 <= year <= 2019 and len(available) >= n - 1):
                        chosen = [sender] + random.sample(available, n - 1)
                        sender["state"] = "Leader"
                        for peer in chosen[1:]:
                            peer["state"] = "InDHT"
                        dht = [p["name"] for p in chosen]
                        waiting = True
                        tuples = [{"name": p["name"], "ip": p["ip"], "p_port": p["p_port"]}
                                  for p in chosen]
                        result = {"status": "SUCCESS", "peers": tuples, "year": year}
                        print(f"DHT SETUP leader={name} peers={dht} year={year}; waiting for completion", flush=True)
                elif command == "dht-complete":
                    if authorized and waiting and dht and name == dht[0]:
                        waiting = False
                        result = {"status": "SUCCESS"}
                        print(f"DHT COMPLETE leader={name} peers={dht}", flush=True)
                else:
                    result["reason"] = "command unavailable in milestone version"

                result["request_id"] = request_id
                cache[key] = result
                reply(sock, addr, result)
                print(f"SEND {result['status']} for {command} to {addr}", flush=True)
            except (UnicodeDecodeError, ValueError, TypeError) as exc:
                print(f"Ignored malformed packet from {addr}: {exc}", flush=True)


if __name__ == "__main__":
    main()
