import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

START, END, DATA, ACK = 0, 1, 2, 3
HEADER_SIZE = 16
MAX_PACKET = 1472
MAX_DATA = MAX_PACKET - HEADER_SIZE
TIMEOUT = 0.1

def make_packet(pkt_type, seq_num, data=b""):
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(data))
    header.checksum = 0
    header.checksum = compute_checksum(header / data)
    return bytes(header / data)

def parse_packet(raw):
    if len(raw) < HEADER_SIZE:
        return None
    header = PacketHeader(raw[:HEADER_SIZE])
    data = raw[HEADER_SIZE : HEADER_SIZE + header.length]
    if len(data) != header.length:
        return None
    recv_checksum = header.checksum
    header.checksum = 0
    if compute_checksum(header / data) != recv_checksum:
        return None
    return header.type, header.seq_num, data

def recv_ack(sock, timeout):
    if timeout <= 0:
        return None
    ready, _, _ = select.select([sock], [], [], timeout)
    if not ready:
        return None
    try:
        raw, _ = sock.recvfrom(2048)
    except OSError:
        return None
    parsed = parse_packet(raw)
    if parsed is None or parsed[0] != ACK:
        return None
    return parsed[1]

def sender(receiver_ip, receiver_port, window_size):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    addr = (receiver_ip, receiver_port)

    message = sys.stdin.buffer.read()
    if len(message) == 0:
        chunks = [b""]
    else:
        chunks = [message[i : i + MAX_DATA] for i in range(0, len(message), MAX_DATA)]

    packets = [make_packet(DATA, i + 1, c) for i, c in enumerate(chunks)]
    n = len(packets)

    # 1. Gửi START
    start_pkt = make_packet(START, 0)
    while True:
        sock.sendto(start_pkt, addr)
        deadline = time.time() + TIMEOUT
        got_ack = False
        while time.time() < deadline:
            if recv_ack(sock, deadline - time.time()) == 1:
                got_ack = True
                break
        if got_ack:
            break

    # 2. Truyền DATA (Selective Repeat)
    base = 1
    next_seq = 1
    acked = {i: False for i in range(1, n + 1)}
    send_times = {i: 0 for i in range(1, n + 1)}

    while base <= n:
        # Gửi các gói chưa gửi trong cửa sổ
        while next_seq < base + window_size and next_seq <= n:
            sock.sendto(packets[next_seq - 1], addr)
            send_times[next_seq] = time.time()
            next_seq += 1

        # Gửi lại CHỈ NHỮNG GÓI bị timeout
        current_time = time.time()
        for seq in range(base, next_seq):
            if not acked[seq] and current_time - send_times[seq] > TIMEOUT:
                sock.sendto(packets[seq - 1], addr)
                send_times[seq] = time.time()

        # Nhận ACK
        ack = recv_ack(sock, 0.01) # Chờ rất ngắn để vòng lặp quét liên tục
        if ack is not None and base <= ack <= n:
            acked[ack] = True
            if ack == base:
                # Trượt cửa sổ nếu gói đầu tiên (base) đã được ACK
                while base <= n and acked[base]:
                    base += 1

    # 3. Gửi END
    end_pkt = make_packet(END, n + 1)
    for _ in range(20):
        sock.sendto(end_pkt, addr)
        deadline = time.time() + TIMEOUT
        got_ack = False
        while time.time() < deadline:
            ack = recv_ack(sock, deadline - time.time())
            if ack is not None and ack >= n + 2:
                got_ack = True
                break
        if got_ack:
            break

    sock.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="IP")
    parser.add_argument("receiver_port", type=int, help="Port")
    parser.add_argument("window_size", type=int, help="Window")
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)

if __name__ == "__main__":
    main()