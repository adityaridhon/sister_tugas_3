import asyncio
from src.communication.failure_detector import FailureDetector

class RaftState:
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"

class RaftNodeConsensus:
    def __init__(self):
        self.state = RaftState.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.votes_received = 0
        self.election_in_progress = False
        
        self.failure_detector = FailureDetector()

    def _is_self_peer(self, peer):
        host = peer.split(":", 1)[0].strip().lower()
        return host == self.node_id.strip().lower()

    def _cluster_size(self):
        # Hitung ukuran cluster berdasarkan host peer unik + node sendiri.
        nodes = {self.node_id.strip().lower()}
        for peer in self.peers:
            host = peer.split(":", 1)[0].strip().lower()
            if host:
                nodes.add(host)
        return len(nodes)

    async def _send_to_peer(self, peer, message, timeout_sec):
        try:
            return await asyncio.wait_for(self.send_message(peer, message), timeout=timeout_sec)
        except Exception:
            return None

    async def start_election(self):
        if self.election_in_progress:
            return

        self.election_in_progress = True
        self.state = RaftState.CANDIDATE
        self.term += 1
        self.voted_for = self.node_id
        self.votes_received = 1
        # Mulai pemilu baru: leader lama tidak lagi valid.
        if hasattr(self, "current_leader"):
            self.current_leader = None
        self.failure_detector.reset_timer()
        
        print(f"[{self.node_id}] Memulai pemilu Term {self.term}...")
        asyncio.create_task(self.request_votes())

    async def request_votes(self):
        """Mengirim pesan RequestVote ke semua node tetangga"""
        try:
            message = {
                "type": "RequestVote",
                "term": self.term,
                "candidate_id": self.node_id
            }

            tasks = []
            for peer in self.peers:
                if self._is_self_peer(peer):
                    continue
                tasks.append(self._send_to_peer(peer, message, timeout_sec=1.0))

            # Tunggu semua node balas (node mati akan timeout cepat, tidak menggantung).
            responses = await asyncio.gather(*tasks, return_exceptions=False)

            # Suara se7 = plus 1
            for resp in responses:
                if resp and resp.get("vote_granted"):
                    self.votes_received += 1

            # Cek mayoritas berdasarkan ukuran cluster (tetap valid saat 1 node down).
            majority = self._cluster_size() // 2 + 1

            if self.votes_received >= majority and self.state == RaftState.CANDIDATE:
                print(f"[{self.node_id}] Node ini yang kepilih menjadi leader {self.term}.")
                self.state = RaftState.LEADER
                if hasattr(self, "current_leader"):
                    self.current_leader = self.node_id
                self.failure_detector.reset_timer()
                asyncio.create_task(self.send_heartbeats())
        finally:
            self.election_in_progress = False

    async def send_heartbeats(self):
        """Tugas Leader: Terus-menerus kirim detak biar follower gak bikin pemilihan lagi"""
        print(f"[{self.node_id}] pengiriman ke semua node...")
        while self.state == RaftState.LEADER:
            message = {
                "type": "Heartbeat",
                "term": self.term,
                "leader_id": self.node_id
            }
            
            # Broadcast
            tasks = [
                self._send_to_peer(peer, message, timeout_sec=0.3)
                for peer in self.peers
                if not self._is_self_peer(peer)
            ]
            await asyncio.gather(*tasks, return_exceptions=False)
            
            # Istirahat 1 detik
            await asyncio.sleep(1.0) 

    async def election_timer(self):
        """Looping background untuk mengecek Leader hidup or not"""
        while True:
            await asyncio.sleep(0.1)
            if self.state != RaftState.LEADER:
                if self.failure_detector.is_leader_dead():
                    # Timeout berikutnya diatur oleh failure_detector.reset_timer().
                    await self.start_election()