"""
FCP v4 server (UDP).

Wire format (matches src/protocols/fcp_client_v4.py on the drone side):

  Main header (16 bytes, big-endian):
      3s magic        = b"FC4"
      B  version      = 4
      B  msg_type     0x01 telemetry, 0x02 image_fragment, 0x04 cfm
      B  flags        0x01 COMPRESSED, 0x10 NEEDS_CFM, 0x20 WITH_METADATA
      H  sequence
      B  api_key_len
      I  payload_len  (bytes after the header, including api_key)
      3s reserved

  After the header: api_key bytes, then body.

  Telemetry body: JSON sensor dict, optionally zlib-compressed.

  Image fragment body:
      I  image_id
      H  total_frags
      H  frag_idx
      H  meta_len
      meta_bytes      (only on frag_idx == 0 with WITH_METADATA flag)
      chunk

The server reassembles fragments per (api_key, image_id), decompresses if
the COMPRESSED flag was set, and passes the payload to a user-provided handler.
"""

import base64
import json
import socket
import struct
import threading
import time
import zlib
from collections import defaultdict
from datetime import datetime

import logging

logger = logging.getLogger(__name__)

class FCPv4Handler:
    """Base handler class for FCPv4 server callbacks."""
    def on_telemetry(self, api_key: str, data: list, received_at: datetime):
        pass
        
    def on_image(self, api_key: str, image_bytes: bytes, meta: dict, received_at: datetime):
        pass


class FCPv4:
    MAGIC = b"FC4"
    VERSION = 4
    HEADER_SIZE = 13
    HEADER_FMT = "!3sBBBHBI"  # 13 bytes, no trailing Reserved/padding

    class MsgType:
        TELEMETRY = 0x01
        IMAGE_FRAGMENT = 0x02
        ACK = 0x04
        NACK = 0x05  # selective retransmit request (body: image_id+total_frags+bitmap)

    class Flags:
        NONE = 0x00
        COMPRESSED = 0x01
        NEEDS_ACK = 0x10
        WITH_METADATA = 0x20

    @staticmethod
    def unpack_header(data):
        magic, ver, mtype, flags, seq, key_len, payload_len = struct.unpack(
            FCPv4.HEADER_FMT, data[:FCPv4.HEADER_SIZE]
        )
        return {
            "magic": magic, "version": ver, "type": mtype,
            "flags": flags, "sequence": seq,
            "api_key_len": key_len, "payload_len": payload_len,
        }

    @staticmethod
    def pack_cfm(sequence):
        return struct.pack(
            FCPv4.HEADER_FMT,
            FCPv4.MAGIC, FCPv4.VERSION,
            FCPv4.MsgType.CFM, FCPv4.Flags.NONE, sequence,
            0, 0,
        )


FRAG_HDR_FMT = "!IHHH"
FRAG_HDR_SIZE = struct.calcsize(FRAG_HDR_FMT)
IMAGE_REASSEMBLY_TIMEOUT_S = 60.0  # was 30 — large images on slow links need longer
RECENT_COMPLETE_TTL_S = 60.0       # remember completed images for CFM re-send on retransmit
SRT_QUIET_PERIOD_MIN_S = 0.5      # NACK after this gap for small (≤ ~200 frag) images
SRT_QUIET_PERIOD_MAX_S = 3.0      # NACK after this gap for huge (1000+ frag) images
SRT_MAX_ROUNDS = 5                # max NACKs per image before giving up
                                   # (bumped from 3 — large images on lossy
                                   # paths need more selective-retransmit
                                   # rounds for the missing-set to converge)
SRT_BODY_FMT = "!IH"              # image_id + total_frags, then bitmap bytes


def _srt_quiet_period_for(total_frags):
    """Scale the SRT debounce window with image fragment count so the server
    does not fire a premature SRT while the slow initial burst from a 2 MB
    image is still arriving (~1400 fragments, several seconds end-to-end on
    a typical uplink)."""
    if total_frags <= 100:
        return SRT_QUIET_PERIOD_MIN_S
    if total_frags >= 1000:
        return SRT_QUIET_PERIOD_MAX_S
    # Linear ramp between the two anchors.
    span = SRT_QUIET_PERIOD_MAX_S - SRT_QUIET_PERIOD_MIN_S
    frac = (total_frags - 100) / (1000 - 100)
    return SRT_QUIET_PERIOD_MIN_S + span * frac


class FCPv4Service:
    """FCP v4 UDP server with DB persistence."""

    def __init__(self, handler: FCPv4Handler = None, host: str = "0.0.0.0", port: int = 4424):
        self.handler = handler or FCPv4Handler()
        self.host = host
        self.port = port
        self.sock = None
        self.running = False
        self._buffers = defaultdict(dict)
        self._buffer_meta = {}
        self._recent_complete = {}  # (api_key, image_id) -> expiry_ts; for dedupe on retransmit
        self._srt_timers = {}      # (api_key, image_id) -> threading.Timer for delayed SRT
        self._lock = threading.Lock()
        
        self.stats = {
            "telemetry_received": 0,
            "images_received": 0,
            "fragments_received": 0,
            "bytes_received": 0,
            "errors": 0,
        }


    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Request a 16 MB receive buffer so a full 2 MB image burst (~1400
        # fragments × ~1500 B = ~2.1 MB on wire) plus headroom for back-to-back
        # images can sit in the kernel queue while the recv loop drains them.
        # NOTE: Linux silently caps this to /proc/sys/net/core/rmem_max — on a
        # stock kernel the cap is ~208 KB (≈140 fragments) which is the actual
        # root cause of FCP v4 0% delivery at 2 MB. To make the bump effective:
        #   sudo sysctl -w net.core.rmem_max=33554432
        #   sudo sysctl -w net.core.rmem_default=16777216
        # And persist in /etc/sysctl.d/99-fcpv4.conf.
        # Verify after start with:  ss -ulnp | grep 4424   (column "Recv-Q")
        # and:                      cat /proc/net/udp | awk '$2 ~ /:1148/ {print}'
        # (1148 = hex(4424)).
        REQUESTED_RCVBUF = 16 * 1024 * 1024
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, REQUESTED_RCVBUF)
        except OSError:
            pass
        try:
            actual_rcvbuf = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            if actual_rcvbuf < REQUESTED_RCVBUF:
                print(f"FCP v4: WARN UDP recv buffer capped at {actual_rcvbuf/1024:.0f} KB "
                      f"(requested {REQUESTED_RCVBUF/1024:.0f} KB). Raise net.core.rmem_max "
                      f"to recover 2 MB image deliveries — see fcp_service_v4.py header.")
        except OSError:
            pass
        self.sock.bind((self.host, self.port))
        self.running = True
        print(f"FCP v4 Service started on {self.host}:{self.port} (UDP)")

        threading.Thread(target=self._gc_loop, daemon=True).start()

        while self.running:
            try:
                datagram, addr = self.sock.recvfrom(65535)
            except OSError:
                if self.running:
                    self.stats["errors"] += 1
                continue

            self.stats["bytes_received"] += len(datagram)
            if len(datagram) < FCPv4.HEADER_SIZE:
                self.stats["errors"] += 1
                continue
            try:
                hdr = FCPv4.unpack_header(datagram)
            except struct.error:
                self.stats["errors"] += 1
                continue
            if hdr["magic"] != FCPv4.MAGIC or hdr["version"] != FCPv4.VERSION:
                self.stats["errors"] += 1
                continue

            tail = datagram[FCPv4.HEADER_SIZE:]
            if len(tail) < hdr["payload_len"]:
                self.stats["errors"] += 1
                continue
            api_key = tail[:hdr["api_key_len"]].decode("ascii", errors="replace")
            body = tail[hdr["api_key_len"]:hdr["payload_len"]]


            if hdr["type"] == FCPv4.MsgType.TELEMETRY:
                self._handle_telemetry(addr, hdr, api_key, body)
            elif hdr["type"] == FCPv4.MsgType.IMAGE_FRAGMENT:
                self._handle_image_fragment(addr, hdr, api_key, body)

    def stop(self):
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        print(f"FCP v4 Service stopped. Stats: {self.stats}")

    def _send_cfm(self, addr, sequence):
        try:
            self.sock.sendto(FCPv4.pack_cfm(sequence), addr)
        except Exception:
            pass

    def _send_srt(self, key):
        """Compose a SRT with bitmap of received-fragment indices and send it
        back to the original sender. Body layout:
            image_id (4 bytes, big-endian)
            total_frags (2 bytes, big-endian)
            bitmap of ceil(total/8) bytes (LSB-first within each byte;
            bit i = 1 means fragment i was received)
        """
        with self._lock:
            meta = self._buffer_meta.get(key)
            if meta is None:
                return  # already complete or expired
            received_idxs = set(self._buffers.get(key, {}).keys())
            total = meta["total"]
            addr = meta["addr"]
            sequence = meta["sequence"]
            api_key = key[0]
            image_id = key[1]
            meta["srt_rounds"] = meta.get("srt_rounds", 0) + 1
            self._srt_timers.pop(key, None)

        bitmap_len = (total + 7) // 8
        bitmap = bytearray(bitmap_len)
        for i in received_idxs:
            if 0 <= i < total:
                bitmap[i // 8] |= (1 << (i % 8))

        body = struct.pack(SRT_BODY_FMT, image_id, total) + bytes(bitmap)
        api_key_bytes = api_key.encode("ascii")
        api_key_len = len(api_key_bytes)
        header = struct.pack(
            FCPv4.HEADER_FMT,
            FCPv4.MAGIC, FCPv4.VERSION,
            FCPv4.MsgType.SRT, FCPv4.Flags.NONE, sequence,
            api_key_len, api_key_len + len(body),
        )
        try:
            self.sock.sendto(header + api_key_bytes + body, addr)
            missing = total - len(received_idxs)
            print(f"FCP v4 SRT: img_id={image_id} missing={missing}/{total} "
                  f"round={self._buffer_meta[key]['srt_rounds']}/{SRT_MAX_ROUNDS}")
        except Exception as e:
            print(f"FCP v4 SRT send failed: {e}")

    def _calc_latency_ms(self, sent_at_str, received_at):
        if not sent_at_str:
            return 0
        try:
            sent_at = datetime.fromisoformat(sent_at_str.replace("Z", "+00:00"))
            if sent_at.tzinfo is not None:
                sent_at = sent_at.replace(tzinfo=None)
            return max(0, (received_at - sent_at).total_seconds() * 1000)
        except Exception:
            return 0

    def _handle_telemetry(self, addr, hdr, api_key, body):
        try:
            if hdr["flags"] & FCPv4.Flags.COMPRESSED:
                body = zlib.decompress(body)
            data = json.loads(body.decode("utf-8"))
        except Exception as e:
            self.stats["errors"] += 1
            print(f"FCP v4 telemetry decode error from {addr}: {e}")
            return
            
        items = data if isinstance(data, list) else [data]
        self.stats["telemetry_received"] += len(items)
        
        self.handler.on_telemetry(api_key, items, datetime.now())
        
        if hdr["flags"] & FCPv4.Flags.NEEDS_ACK:
            self._send_cfm(addr, hdr["sequence"])

        items = data if isinstance(data, list) else [data]
        received_at = datetime.now()
        
        self.stats["telemetry_received"] += len(items)
        self.handler.on_telemetry(api_key, items, received_at)

        if hdr["flags"] & FCPv4.Flags.NEEDS_ACK:
            self._send_cfm(addr, hdr["sequence"])

    def _handle_image_fragment(self, addr, hdr, api_key, body):
        if len(body) < FRAG_HDR_SIZE:
            self.stats["errors"] += 1
            return
        image_id, total, idx, meta_len = struct.unpack(FRAG_HDR_FMT, body[:FRAG_HDR_SIZE])
        cursor = FRAG_HDR_SIZE
        meta_bytes = body[cursor:cursor + meta_len]
        cursor += meta_len
        chunk = body[cursor:]
        self.stats["fragments_received"] += 1

        key = (api_key, image_id)

        # Dedupe: if this image already completed recently, just re-CFM without
        # accumulating fragments or re-saving to DB. Lets the client retransmit
        # safely when a CFM is lost on the return path.
        with self._lock:
            exp = self._recent_complete.get(key)
            if exp and exp > time.time():
                self._send_cfm(addr, hdr["sequence"])
                return

        completed_payload = None
        completed_meta = None
        completed_flags = 0
        completed_seq = hdr["sequence"]

        with self._lock:
            self._buffers[key][idx] = chunk
            existing = self._buffer_meta.get(key)
            if existing is None:
                existing = {
                    "total": total, "meta": None, "flags": hdr["flags"],
                    "sequence": hdr["sequence"],
                    "started_at": time.time(),
                    "addr": addr,
                    "srt_rounds": 0,
                }
                self._buffer_meta[key] = existing
            existing["flags"] |= hdr["flags"]
            existing["addr"] = addr  # remember sender for SRT return path
            if hdr["flags"] & FCPv4.Flags.WITH_METADATA and meta_bytes:
                try:
                    existing["meta"] = json.loads(meta_bytes.decode("utf-8"))
                except Exception:
                    pass

            if len(self._buffers[key]) == existing["total"]:
                buf = self._buffers.pop(key)
                completed_meta = existing["meta"] or {}
                completed_flags = existing["flags"]
                completed_seq = existing["sequence"]
                self._buffer_meta.pop(key, None)
                completed_payload = b"".join(buf[i] for i in range(existing["total"]))
                # Cancel any pending SRT timer — image is complete.
                pending = self._srt_timers.pop(key, None)
                if pending is not None:
                    pending.cancel()
            else:
                # Schedule a delayed SRT if no further fragments arrive within
                # the quiet period. Cancel any existing one first so the timer
                # always fires SRT_QUIET_PERIOD_S after the *last* fragment.
                pending = self._srt_timers.pop(key, None)
                if pending is not None:
                    pending.cancel()
                if existing["srt_rounds"] < SRT_MAX_ROUNDS:
                    quiet = _srt_quiet_period_for(existing["total"])
                    timer = threading.Timer(quiet, self._send_srt, args=(key,))
                    timer.daemon = True
                    self._srt_timers[key] = timer
                    timer.start()

        if completed_payload is None:
            # Image still assembling. Skip CFM during accumulation to keep return
            # path quiet — completion CFM is what client waits for.
            return

        if completed_flags & FCPv4.Flags.COMPRESSED:
            try:
                completed_payload = zlib.decompress(completed_payload)
            except Exception as e:
                self.stats["errors"] += 1
                print(f"FCP v4 image decompress failed: {e}")
                return

        received_at = datetime.now()
        latency_ms = self._calc_latency_ms(completed_meta.get("sent_at"), received_at)

        # Fire callback
        self.stats["images_received"] += 1
        print(f"FCP v4 image completed: seq={completed_seq} "
              f"size={len(completed_payload) / 1024:.1f}KB "
              f"latency={latency_ms:.2f}ms")
        self.handler.on_image(api_key, completed_payload, completed_meta, received_at)

        # Always send completion CFM when image is fully reassembled and saved,
        # regardless of NEEDS_ACK flag on individual fragments. Client uses this
        # to decide whether to retransmit the whole image.
        with self._lock:
            self._recent_complete[(api_key, image_id)] = time.time() + RECENT_COMPLETE_TTL_S
        self._send_cfm(addr, hdr["sequence"])

    def _gc_loop(self):
        while self.running:
            time.sleep(5.0)
            now = time.time()
            with self._lock:
                stale = [k for k, m in self._buffer_meta.items()
                         if now - m["started_at"] > IMAGE_REASSEMBLY_TIMEOUT_S]
                for k in stale:
                    self._buffers.pop(k, None)
                    self._buffer_meta.pop(k, None)
                    pending_timer = self._srt_timers.pop(k, None)
                    if pending_timer is not None:
                        pending_timer.cancel()
                expired = [k for k, exp in self._recent_complete.items() if exp <= now]
                for k in expired:
                    self._recent_complete.pop(k, None)
            if stale:
                print(f"FCP v4: dropped {len(stale)} stale image buffers")
