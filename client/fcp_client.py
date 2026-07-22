"""
Flight Communication Protocol (FCP) - Client
An application-layer protocol optimized for multimodal UAV-IoT transmission over
cellular networks. Features a dual-mode UDP transport with multi-channel fan-out
for large payloads (images) and single-datagram delivery for telemetry.

Features:
- UDP transport: eliminates TCP handshake and per-message CFM round-trip.
- Multi-channel fan-out for images: parallel UDP sockets to saturate the send path.
- JPEG/PNG passthrough: avoids double-compressing image payloads.
- Selective Retransmission: uses a bitmap-based NACK system.
"""

import os
import socket
import select
import struct
import time
import json
import zlib
import threading
from typing import Tuple

_JPEG_MAGIC = b"\xff\xd8\xff"
_PNG_MAGIC = b"\x89PNG"


class FCPv4:
    MAGIC = b"FC4"
    VERSION = 4
    HEADER_SIZE = 13

    class MsgType:
        TELEMETRY = 0x01
        IMAGE_FRAGMENT = 0x02
        CFM = 0x04
        SRT = 0x05  # selective retransmit request — body has bitmap of received indices

    class Flags:
        NONE = 0x00
        COMPRESSED = 0x01
        NEEDS_CFM = 0x10
        WITH_METADATA = 0x20

    HEADER_FMT = "!3sBBBHBI"  # 13 bytes, no trailing Reserved/padding
    # Precompile the struct so per-fragment pack() skips format-string parsing.
    # Hot path: a 2 MB image emits ~1400 fragments × this call.
    _HEADER_STRUCT = struct.Struct(HEADER_FMT)
    _FRAG_STRUCT = struct.Struct("!IHHH")
    _SRT_PREFIX_STRUCT = struct.Struct("!IH")
    FRAG_HDR_SIZE = _FRAG_STRUCT.size

    @staticmethod
    def pack_header(msg_type, flags, sequence, api_key_len, payload_len):
        return FCPv4._HEADER_STRUCT.pack(
            FCPv4.MAGIC, FCPv4.VERSION,
            msg_type, flags, sequence,
            api_key_len, payload_len,
        )


class FCPv4Client:
    """UDP-based FCP v4 client. Best-effort by default."""

    SAFE_PACKET_SIZE = 1450
    NUM_IMAGE_CHANNELS = 20
    SOCKET_SNDBUF = 1024 * 1024

    def __init__(self, host: str, port: int, api_key: str = None):
        self.host = host
        self.port = port
        self.api_key = (api_key or "").encode("ascii")
        self.api_key_len = len(self.api_key)
        if self.api_key_len > 255:
            raise ValueError("api_key too long for 1-byte length field")

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.SOCKET_SNDBUF)
        except OSError:
            pass
        self.sock.settimeout(1.0)
        # Seed the wire sequence from wall-clock time + pid to keep image_ids unique
        # across different sessions or consecutive runs that share an api_key.
        self.sequence = (int(time.time() * 1000) + os.getpid()) % 65536
        self.connected = True

        # Persistent socket pool for image fan-out. Reused across send_image
        # calls to avoid the overhead of opening sockets on every image.
        # Cheap to keep idle; the only cost is NUM_IMAGE_CHANNELS file descriptors.
        self._image_socks = []
        for _ in range(self.NUM_IMAGE_CHANNELS):
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.SOCKET_SNDBUF)
            except OSError:
                pass
            self._image_socks.append(s)

        print(f"FCP v4: UDP send target {host}:{port} (image pool={self.NUM_IMAGE_CHANNELS})")

    def _next_seq(self):
        s = self.sequence
        self.sequence = (self.sequence + 1) % 65536
        return s

    def send_sensor_json(self, sensor_data, wait_cfm: bool = False) -> Tuple[bool, float]:
        """Send sensor data as a single UDP datagram. Returns (ok, latency_ms).
        Packs the header, encodes payload, performs a single sendto, and
        does an optional CFM wait. No compression check — sensor payloads
        are sub-KB JSON; zlib at this size is dominated by its constant
        setup cost.
        """
        try:
            payload = json.dumps(sensor_data, separators=(",", ":")).encode("utf-8")
            flags = FCPv4.Flags.NEEDS_CFM if wait_cfm else FCPv4.Flags.NONE

            seq = self._next_seq()
            header = FCPv4._HEADER_STRUCT.pack(
                FCPv4.MAGIC, FCPv4.VERSION,
                FCPv4.MsgType.TELEMETRY, flags, seq,
                self.api_key_len, self.api_key_len + len(payload),
            )
            datagram = b"".join((header, self.api_key, payload))

            t0 = time.time()
            self.sock.sendto(datagram, (self.host, self.port))

            if wait_cfm:
                try:
                    self.sock.settimeout(2.0)
                    self.sock.recv(FCPv4.HEADER_SIZE)
                except socket.timeout:
                    return False, (time.time() - t0) * 1000

            return True, (time.time() - t0) * 1000
        except Exception as e:
            print(f"FCP v4 telemetry error: {e}")
            return False, 0.0

    def send_image(self, image_bytes: bytes, metadata: dict,
                   wait_cfm: bool = False, max_srt_rounds: int = 5,
                   cfm_timeout: float = 2.0) -> Tuple[bool, float, int, str]:
        """
        Multi-channel UDP fan-out for image transfer with SRT-driven recovery.

        Uses a contiguous range of fragment indices per channel, one plain
        threading.Thread per channel, and datagram construction inlined into
        the channel's send loop (no per-fragment closure call) to save CPU.

        wait_cfm=True enables image-level reliability:
          1. Client sends all fragments via fan-out.
          2. Server reassembles; on quiet_period of no new fragments without
             completion, returns a SRT whose body carries a bitmap of which
             fragments arrived. The client retransmits only the missing ones.
          3. Repeats until completion CFM or max_srt_rounds SRTs.

        Returns (ok, latency_ms, fragments_sent, message).
        """
        try:
            already_coded = image_bytes.startswith(_JPEG_MAGIC) or image_bytes.startswith(_PNG_MAGIC)

            payload = image_bytes
            base_flags = FCPv4.Flags.NONE
            if not already_coded and len(image_bytes) > 10240:
                compressed = zlib.compress(image_bytes, level=1)
                if len(compressed) < len(image_bytes):
                    payload = compressed
                    base_flags |= FCPv4.Flags.COMPRESSED

            # Mutate `metadata` in place (caller is expected to discard the
            # dict after the call). Skipping the dict(metadata) copy used to
            # allocate a fresh dict on every send_image and bumped CPU under
            # high image rates.
            md = metadata
            md.pop("api_key", None)
            seq = self._next_seq()
            md["sequence"] = seq
            meta_bytes = json.dumps(md, separators=(",", ":")).encode("utf-8")

            hdr_struct = FCPv4._HEADER_STRUCT
            frag_struct = FCPv4._FRAG_STRUCT
            frag_hdr_size = FCPv4.FRAG_HDR_SIZE

            image_id = seq
            payload_len = len(payload)
            payload_mv = memoryview(payload)

            base_overhead = FCPv4.HEADER_SIZE + self.api_key_len + frag_hdr_size
            meta_len = len(meta_bytes)
            chunk_size_first = max(256, self.SAFE_PACKET_SIZE - base_overhead - meta_len)
            chunk_size_others = max(256, self.SAFE_PACKET_SIZE - base_overhead)

            if payload_len <= chunk_size_first:
                total_frags = 1
            else:
                rest = payload_len - chunk_size_first
                total_frags = 1 + (rest + chunk_size_others - 1) // chunk_size_others

            # Capture into locals so the threaded send loop avoids attribute
            # lookups on `self` and the FCPv4 class on every fragment.
            api_key_bytes = self.api_key
            api_key_len = self.api_key_len
            magic = FCPv4.MAGIC
            version = FCPv4.VERSION
            msg_image = FCPv4.MsgType.IMAGE_FRAGMENT
            flag_with_meta = FCPv4.Flags.WITH_METADATA
            addr = (self.host, self.port)

            # Pacing: emit a per-fragment sleep for any image >50 fragments
            # (~70 KB), with 2 ms gap per fragment within each channel.
            # Rationale: live runs against the production VM showed that
            # the SRT return path (server → client UDP) is unreliable
            # under residential NAT, so depending on SRT recovery left
            # FCP v4 image delivery at 0 % for 500 KB+ and 1 MB+ even
            # though the sensor path (one-way UDP) worked fine. Aggressive
            # pacing on the send side cuts the initial-burst fragment
            # loss to near zero, so the image completes on the first
            # round without needing the SRT loop at all.
            #
            # Per-channel effective rate at 2 ms gap × 1450 B fragment:
            #   5.8 Mbps per channel, ~116 Mbps theoretical across 20
            #   channels but actually capped by the residential uplink.
            # Per-server arrival rate stays low enough that the gateway
            # recv loop drains without buffer overflow even before the
            # rmem_max sysctl fix.
            #
            # Wall-time cost (per channel handles total_frags/num_channels):
            #   500 KB (~357 frags / 20 ch = 18 per ch): 36 ms pacing
            #   1   MB (~740 frags / 20 ch = 37 per ch): 74 ms pacing
            #   2   MB (~1416 frags / 20 ch = 71 per ch): 142 ms pacing
            # Added to the actual sendto time, total send wall time
            # roughly doubles vs the old 200 µs pacing, but delivery
            # rate goes from 0 % to ~95 %+ — a worthwhile trade.
            PACING_THRESHOLD = 50
            PACING_DELAY_S = 0.002
            pace_sleep = PACING_DELAY_S if total_frags > PACING_THRESHOLD else 0.0

            def send_channel_range(channel_index, fragment_indices):
                """Send the given contiguous (or arbitrary) fragment indices
                on channel `channel_index`. Datagram construction inlined."""
                s = self._image_socks[channel_index]
                for fi in fragment_indices:
                    if fi == 0:
                        data_start = 0
                        data_end = min(chunk_size_first, payload_len)
                        this_meta = meta_bytes
                        this_meta_len = meta_len
                        flags = base_flags | flag_with_meta
                    else:
                        offset = chunk_size_first + (fi - 1) * chunk_size_others
                        data_start = offset
                        data_end = min(offset + chunk_size_others, payload_len)
                        this_meta = b""
                        this_meta_len = 0
                        flags = base_flags
                    body_len = frag_hdr_size + this_meta_len + (data_end - data_start)
                    main_hdr = hdr_struct.pack(
                        magic, version,
                        msg_image, flags, seq,
                        api_key_len, api_key_len + body_len,
                    )
                    frag_hdr = frag_struct.pack(image_id, total_frags, fi, this_meta_len)
                    packet = b"".join((
                        main_hdr, api_key_bytes, frag_hdr, this_meta,
                        payload_mv[data_start:data_end],
                    ))
                    try:
                        s.sendto(packet, addr)
                    except Exception:
                        pass
                    if pace_sleep:
                        time.sleep(pace_sleep)

            def fan_out_contiguous(num_total):
                """Initial burst: each channel gets a contiguous range of
                fragment indices. This approach empirically uses less CPU
                than per-fragment round-robin because the inner loop reduces
                to a single range() iterator rather than a list of indices."""
                num_channels = min(self.NUM_IMAGE_CHANNELS, num_total)
                per_ch = (num_total + num_channels - 1) // num_channels
                threads = []
                for c in range(num_channels):
                    start = c * per_ch
                    end = min(start + per_ch, num_total)
                    if start >= end:
                        continue
                    t = threading.Thread(
                        target=send_channel_range,
                        args=(c, range(start, end)),
                        daemon=True,
                    )
                    threads.append(t)
                    t.start()
                for t in threads:
                    t.join()

            def fan_out_indices(indices):
                """SRT retransmit path: fragment list isn't contiguous, so
                we round-robin across channels to spread the resends."""
                if not indices:
                    return
                num_channels = min(self.NUM_IMAGE_CHANNELS, len(indices))
                buckets = [[] for _ in range(num_channels)]
                for n, fi in enumerate(indices):
                    buckets[n % num_channels].append(fi)
                threads = []
                for c, fis in enumerate(buckets):
                    if not fis:
                        continue
                    t = threading.Thread(
                        target=send_channel_range,
                        args=(c, fis),
                        daemon=True,
                    )
                    threads.append(t)
                    t.start()
                for t in threads:
                    t.join()

            def drain_pool_sockets():
                """Discard any stale datagrams sitting in pool socket buffers."""
                for s in self._image_socks:
                    s.setblocking(False)
                    try:
                        while True:
                            s.recv(4096)
                    except (BlockingIOError, OSError):
                        pass
                    finally:
                        s.setblocking(True)

            def wait_for_response(timeout):
                """Block up to `timeout` for a CFM or SRT with matching seq.
                Returns ('CFM', None), ('SRT', missing_indices_list), or
                ('TIMEOUT', None)."""
                deadline = time.time() + timeout
                while True:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        return ("TIMEOUT", None)
                    ready, _, _ = select.select(self._image_socks, [], [], remaining)
                    for s in ready:
                        try:
                            data, _ = s.recvfrom(8192)
                        except OSError:
                            continue
                        if len(data) < FCPv4.HEADER_SIZE:
                            continue
                        try:
                            m, _v, mt, _f, ack_seq, key_len, payload_len_h = \
                                FCPv4._HEADER_STRUCT.unpack(data[:FCPv4.HEADER_SIZE])
                        except struct.error:
                            continue
                        if m != FCPv4.MAGIC or ack_seq != seq:
                            continue
                        if mt == FCPv4.MsgType.CFM:
                            return ("CFM", None)
                        if mt == FCPv4.MsgType.SRT:
                            tail = data[FCPv4.HEADER_SIZE:]
                            body = tail[key_len:payload_len_h]
                            if len(body) < FCPv4._SRT_PREFIX_STRUCT.size:
                                continue
                            _img_id, total_in_srt = FCPv4._SRT_PREFIX_STRUCT.unpack(
                                body[:FCPv4._SRT_PREFIX_STRUCT.size]
                            )
                            bitmap = body[FCPv4._SRT_PREFIX_STRUCT.size:]
                            missing = []
                            for i in range(total_in_srt):
                                byte = bitmap[i // 8] if (i // 8) < len(bitmap) else 0
                                if not (byte & (1 << (i % 8))):
                                    missing.append(i)
                            return ("SRT", missing)

            t0 = time.time()

            if not wait_cfm:
                fan_out_contiguous(total_frags)
                latency_ms = (time.time() - t0) * 1000
                return True, latency_ms, total_frags, "OK"

            drain_pool_sockets()
            fan_out_contiguous(total_frags)

            total_resent = 0
            for srt_round in range(max_srt_rounds + 1):
                result, payload_data = wait_for_response(cfm_timeout)
                if result == "CFM":
                    latency_ms = (time.time() - t0) * 1000
                    msg = "CFM on first burst" if srt_round == 0 \
                          else f"CFM after {srt_round} SRT round(s), {total_resent} frags resent"
                    return True, latency_ms, total_frags, msg
                if result == "TIMEOUT":
                    latency_ms = (time.time() - t0) * 1000
                    return False, latency_ms, total_frags, \
                        f"timeout after {srt_round} SRT round(s)"
                # SRT
                missing = payload_data or []
                if not missing:
                    # Server says we have everything — wait one more cycle for CFM.
                    continue
                if srt_round >= max_srt_rounds:
                    latency_ms = (time.time() - t0) * 1000
                    return False, latency_ms, total_frags, \
                        f"max SRT rounds reached, still missing {len(missing)} frags"
                print(f"FCP v4 image: seq={seq} SRT round {srt_round + 1}/{max_srt_rounds} "
                      f"resending {len(missing)} of {total_frags} frags")
                fan_out_indices(missing)
                total_resent += len(missing)

            latency_ms = (time.time() - t0) * 1000
            return False, latency_ms, total_frags, "exhausted SRT rounds"
        except Exception as e:
            return False, 0.0, 0, str(e)

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass
        for s in self._image_socks:
            try:
                s.close()
            except Exception:
                pass
        self._image_socks = []
        self.connected = False
