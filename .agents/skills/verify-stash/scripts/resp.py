#!/usr/bin/env python3
"""Send one RESP command to a server and print the reply in redis-cli style.
Usage: resp.py <host:port> <arg>..."""
import socket, sys

def read_reply(f):
    line = f.readline()
    if not line:
        raise SystemExit("connection closed")
    t, rest = line[:1], line[1:].rstrip(b"\r\n")
    if t == b"+":
        return rest.decode()
    if t == b"-":
        return "(error) " + rest.decode()
    if t == b":":
        return "(integer) " + rest.decode()
    if t == b"$":
        n = int(rest)
        if n < 0:
            return "(nil)"
        data = f.read(n + 2)[:n]
        return '"' + data.decode(errors="replace") + '"'
    if t == b"*":
        n = int(rest)
        if n < 0:
            return "(nil)"
        return "\n".join(f"{i + 1}) {read_reply(f)}" for i in range(n)) or "(empty array)"
    raise SystemExit(f"unexpected reply type {line!r}")

host, port = sys.argv[1].rsplit(":", 1)
args = [a.encode() for a in sys.argv[2:]]
frame = b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args)
with socket.create_connection((host, int(port)), timeout=5) as s:
    s.sendall(frame)
    print(read_reply(s.makefile("rb")))
