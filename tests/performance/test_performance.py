from locust import HttpUser, task, between
import random

class DistributedUser(HttpUser):
    # Waktu tunggu tiap user sebelum nge-hit lagi
    wait_time = between(1, 2)

    @task(3)
    def test_cache(self):
        # Tembak random ke Cache 1, 2, atau 3
        port = random.choice([8001, 8002, 8003])
        # Sesuaikan endpoint dan method dengan API aslimu
        self.client.get(f"http://localhost:{port}/status", name="Cache Nodes")

    @task(2)
    def test_queue(self):
        # Tembak random ke Queue 1, 2, atau 3
        port = random.choice([8004, 8005, 8006])
        self.client.get(f"http://localhost:{port}/status", name="Queue Nodes")

    @task(1)
    def test_lock(self):
        # Tembak random ke Lock 1, 2, atau 3
        port = random.choice([8007, 8008, 8009])
        self.client.get(f"http://localhost:{port}/status", name="Lock Nodes")