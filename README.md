# Flight Communication Protocol (FCP)

FCP is a specialized application-layer protocol optimized for **multimodal UAV--IoT transmission over public 4G/LTE cellular networks**. 

It provides a dual-mode UDP transport to handle two distinct types of data simultaneously:
1. **Telemetry (Sensor Data):** Sent as a single, low-latency datagram without connection overhead.
2. **Images (Large Payloads):** Fragmented and transmitted concurrently using multi-channel fan-out across multiple UDP sockets to saturate lossy wireless uplink paths. 

Instead of relying on the transport layer (like TCP) for slow congestion control, FCP implements an aggressive **bitmap-based Selective Retransmission (NACK)** mechanism. This allows it to instantly recover dropped fragments while bypassing the head-of-line blocking that typically cripples TCP on drone networks.

## Features

- **Zero-Handshake Telemetry:** Packets fly immediately; ideal for 1Hz–10Hz sensor streaming.
- **Multi-Channel UDP Fan-Out:** Spins up parallel channels to maximize throughput on unreliable cellular links.
- **Selective Retransmission (SRT):** Re-requests only the dropped fragments using a compact NACK bitmap.
- **JPEG/PNG Passthrough:** Avoids double-compressing already compressed multimedia.
- **Micro-Footprint:** Built to run on edge compute boards like Nvidia Jetson and Raspberry Pi.

## Installation

FCP is written in pure Python and has no external dependencies. 
Clone the repository and integrate the `client` and `server` folders into your project.

```bash
git clone https://github.com/Fahmi300/FCP-OpenSource.git
cd FCP-OpenSource
```

## Quick Start Example

A complete demonstration is available in `example.py`. 

### Server Initialization

Implement the `FCPv4Handler` interface to receive decoded data. 
The server spins up a threaded UDP listener that will reconstruct the fragmented payloads entirely in the background.

```python
from server.fcp_server import FCPv4Service, FCPv4Handler
import threading
from datetime import datetime

class MyHandler(FCPv4Handler):
    def on_telemetry(self, api_key: str, data: list, received_at: datetime):
        print(f"Telemetry from {api_key}: {data}")
        
    def on_image(self, api_key: str, image_bytes: bytes, meta: dict, received_at: datetime):
        print(f"Image received from {api_key}, size: {len(image_bytes)}")

# Start server
handler = MyHandler()
server = FCPv4Service(handler=handler, host="0.0.0.0", port=4424)
threading.Thread(target=server.start, daemon=True).start()
```

### Client Initialization (UAV side)

```python
from client.fcp_client import FCPv4Client

client = FCPv4Client(host="192.168.1.100", port=4424, api_key="UAV_ALPHA_1")

# 1. Send Telemetry
sensor_data = {"lat": -7.28, "lon": 112.79, "alt": 150}
client.send_sensor_json(sensor_data, wait_cfm=False)

# 2. Send Image (Automatically fragmented and sent via Fan-Out)
with open("drone_cam.jpg", "rb") as f:
    img_bytes = f.read()

client.send_image(img_bytes)
```

## Kernel Optimization for Large Images

If you plan to send images larger than 500 KB over high-speed networks, your operating system's default UDP receive buffer might be too small, causing the kernel to drop fragments before FCP can process them. 

On Linux, increase the max socket buffer size:
```bash
sudo sysctl -w net.core.rmem_max=33554432
sudo sysctl -w net.core.rmem_default=16777216
```

## Citation

If you use FCP in your research, please cite our IEEE Internet of Things Journal paper:

```bibtex
@article{fatwaddin2025fcp,
  title={FCP: A Dual-Mode UDP Protocol With Multi-Channel Fan-Out and Selective Retransmission for Multimodal UAV--IoT Communication Over Public 4G Networks},
  author={Mirda, Irfan and Fatwaddin, Fahmi Anhar and Sarno, Riyanarto and Sungkono, Kelly Rossa},
  journal={IEEE Internet of Things Journal},
  year={2026},
  publisher={IEEE}
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
