import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16
ACK = 3


def make_ack(seq_num):
    header = PacketHeader(type=ACK, seq_num=seq_num, length=0)
    header.checksum = 0
    header.checksum = compute_checksum(header)
    return bytes(header)


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


def receiver(receiver_ip, receiver_port, window_size):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((receiver_ip, receiver_port))

    expected_seq = 0
    buffer = {}
    in_connection = False

    while True:
        try:
            raw, addr = sock.recvfrom(2048)
        except OSError:
            continue

        parsed = parse_packet(raw)
        if parsed is None:
            continue

        pkt_type, seq_num, data = parsed

        if pkt_type == 0:  # START
            if not in_connection:
                in_connection = True
                expected_seq = 1
                buffer.clear()
            sock.sendto(make_ack(1), addr)
            continue

        if not in_connection:
            continue

        if pkt_type == 1:  # END
            if seq_num == expected_seq:
                sock.sendto(make_ack(expected_seq + 1), addr)
                break

        if pkt_type == 2:  # DATA
            if seq_num >= expected_seq + window_size:
                continue

            if seq_num < expected_seq:
                sock.sendto(make_ack(expected_seq), addr)
            elif seq_num == expected_seq:
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
                expected_seq += 1

                while expected_seq in buffer:
                    sys.stdout.buffer.write(buffer.pop(expected_seq))
                    sys.stdout.buffer.flush()
                    expected_seq += 1

                sock.sendto(make_ack(expected_seq), addr)
            else:
                buffer[seq_num] = data
                sock.sendto(make_ack(expected_seq), addr)

    sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("receiver_ip", help="IP")
    parser.add_argument("receiver_port", type=int, help="Port")
    parser.add_argument("window_size", type=int, help="Window Size")
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()