import time
import random
import os

class FailureDetector:
    def __init__(self):
        self.last_heartbeat = time.time()
        self.election_timeout = self._generate_timeout()

    def _generate_timeout(self):
        # Waktu tunggu acak default 1.5 - 3 detik (bisa diubah via env).
        min_timeout = float(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "1.5"))
        max_timeout = float(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "3.0"))
        return random.uniform(min_timeout, max_timeout)

    def reset_timer(self):
        """Panggil ini setiap kali menerima sinyal kehidupan (Heartbeat)"""
        self.last_heartbeat = time.time()
        self.election_timeout = self._generate_timeout()

    def is_leader_dead(self):
        """Cek apakah sudah lewat batas waktu tanpa kabar dari Leader"""
        return (time.time() - self.last_heartbeat) > self.election_timeout