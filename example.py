import time
import threading
from datetime import datetime

# Import FCP Client and Server
from client.fcp_client import FCPv4Client
from server.fcp_server import FCPv4Service, FCPv4Handler

class MockHandler(FCPv4Handler):
    """Custom handler for incoming FCP traffic."""
    
    def on_telemetry(self, api_key: str, data: list, received_at: datetime):
        print(f"\n[SERVER] Telemetry received from '{api_key}':")
        for item in data:
            print(f"         - {item}")
            
    def on_image(self, api_key: str, image_bytes: bytes, meta: dict, received_at: datetime):
        print(f"\n[SERVER] Image received from '{api_key}':")
        print(f"         - Size: {len(image_bytes)} bytes")
        print(f"         - Meta: {meta}")

def run_server():
    handler = MockHandler()
    server = FCPv4Service(handler=handler, host="127.0.0.1", port=4424)
    # Run the server in a separate thread so it doesn't block the example
    threading.Thread(target=server.start, daemon=True).start()
    return server

def run_client():
    # Give server a moment to start
    time.sleep(1)
    
    client = FCPv4Client(host="127.0.0.1", port=4424, api_key="DEMO_API_KEY")
    
    # Send mock telemetry
    print("\n[CLIENT] Sending telemetry data...")
    sensor_data = {
        "temperature": 25.4,
        "humidity": 60,
        "lat": -7.28,
        "lon": 112.79,
        "timestamp": datetime.now().isoformat()
    }
    client.send_sensor_json(sensor_data, wait_cfm=True)
    
    # Send mock image
    print("\n[CLIENT] Sending image data...")
    dummy_image = b"x" * 150000  # 150 KB mock image
    client.send_image(dummy_image, metadata={"filename": "test.jpg", "resolution": "1080p"})
    
    # Wait for transmission to finish
    time.sleep(2)

if __name__ == "__main__":
    print("Starting FCP Demo...")
    srv = run_server()
    run_client()
    srv.stop()
    print("\nDemo finished.")
