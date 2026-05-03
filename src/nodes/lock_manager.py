import asyncio
from aiohttp import web
from src.nodes.base_node import BaseNode
from src.consensus.raft import RaftNodeConsensus, RaftState

class LockManager(BaseNode, RaftNodeConsensus):
    def __init__(self):
        BaseNode.__init__(self)       
        RaftNodeConsensus.__init__(self)  
        
        self.locks = {}
        self.current_leader = None
        
        # --- DAFTAR API ENDPOINT KITA ---
        self.app.router.add_post('/lock', self.acquire_lock)
        self.app.router.add_post('/unlock', self.release_lock)
        self.app.router.add_get('/status', self.get_status) # <--- INI YANG TADI BIKIN 404
        
        self.app.on_startup.append(self.start_background_tasks)

    async def start_background_tasks(self, app):
        asyncio.create_task(self.election_timer())

    def _leader_hint(self):
        # Hindari hint pemimpin ke diri sendiri saat state bukan leader (leader stale).
        if self.current_leader and self.current_leader != self.node_id:
            return self.current_leader
        return None

    # --- API BARU: Untuk Swagger ngecek siapa Leader-nya ---
    async def get_status(self, request):
        return web.json_response({
            "node_id": self.node_id,
            "state": self.state,
            "current_term": self.term,
            "current_leader": self.current_leader
        })

    # --- API LOCK & UNLOCK ---
    async def acquire_lock(self, request):
        if self.state != RaftState.LEADER:
            leader_hint = self._leader_hint()
            if leader_hint:
                pesan = f"Saya hanya Follower. Silakan minta lock ke Leader: {leader_hint}"
            else:
                pesan = "Leader belum diketahui, coba lagi beberapa saat"
            return web.json_response({"status": "gagal", "pesan": pesan}, status=400)
            
        data = await request.json()
        resource = data.get("resource")
        client_id = data.get("client_id")
        lock_type = data.get("lock_type", "exclusive")
        
        current_lock = self.locks.get(resource)
        
        if not current_lock:
            self.locks[resource] = {"type": lock_type, "owners": [client_id]}
            print(f"[{self.node_id}] 🔒 LOCK DIBERIKAN: {client_id} mengunci {resource} ({lock_type})")
            return web.json_response({"status": "sukses", "pesan": "Lock berhasil didapatkan"})
            
        if lock_type == "shared" and current_lock["type"] == "shared":
            if client_id not in current_lock["owners"]:
                current_lock["owners"].append(client_id)
            print(f"[{self.node_id}] 🔒 SHARED LOCK: {client_id} ikut membaca {resource}")
            return web.json_response({"status": "sukses", "pesan": "Shared Lock berhasil didapatkan"})
            
        print(f"[{self.node_id}] ⛔ LOCK DITOLAK: {resource} sedang dipakai oleh {current_lock['owners']}")
        return web.json_response({"status": "gagal", "pesan": "Resource sedang dikunci"}, status=423)

    async def release_lock(self, request):
        if self.state != RaftState.LEADER:
            leader_hint = self._leader_hint()
            if leader_hint:
                pesan = f"Minta ke Leader: {leader_hint}"
            else:
                pesan = "Leader belum diketahui, coba lagi beberapa saat"
            return web.json_response({"status": "gagal", "pesan": pesan}, status=400)
            
        data = await request.json()
        resource = data.get("resource")
        client_id = data.get("client_id")
        
        if resource in self.locks and client_id in self.locks[resource]["owners"]:
            self.locks[resource]["owners"].remove(client_id)
            if not self.locks[resource]["owners"]:
                del self.locks[resource]
            
            print(f"[{self.node_id}] 🔓 UNLOCK: {client_id} melepas {resource}")
            return web.json_response({"status": "sukses", "pesan": "Lock dilepas"})
            
        return web.json_response({"status": "gagal", "pesan": "Anda tidak memiliki lock ini"}, status=400)

    # --- HANDLER PESAN INTERNAL ANTAR NODE ---
    async def handle_message(self, request):
        data = await request.json()
        msg_type = data.get("type")
        
        if msg_type == "RequestVote":
            candidate_term = data.get("term")
            candidate_id = data.get("candidate_id")

            if candidate_term > self.term:
                self.term = candidate_term
                self.state = RaftState.FOLLOWER
                self.voted_for = candidate_id
                # Kandidat belum tentu menang; kosongkan leader lama agar tidak stale.
                self.current_leader = None
                self.failure_detector.reset_timer()
                return web.json_response({"term": self.term, "vote_granted": True})

            if candidate_term == self.term and (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self.failure_detector.reset_timer()
                return web.json_response({"term": self.term, "vote_granted": True})

            return web.json_response({"term": self.term, "vote_granted": False})
            
        elif msg_type == "Heartbeat":
            leader_term = data.get("term")
            leader_id = data.get("leader_id")

            if leader_term > self.term:
                self.term = leader_term
                self.state = RaftState.FOLLOWER
                self.current_leader = leader_id
                self.failure_detector.reset_timer()
                return web.json_response({"status": "ok"})

            if leader_term == self.term:
                # Leader sah pada term yang sama untuk follower/candidate.
                if self.state != RaftState.LEADER:
                    self.state = RaftState.FOLLOWER
                    self.current_leader = leader_id
                    self.failure_detector.reset_timer()
                    return web.json_response({"status": "ok"})

                # Jika node ini leader di term yang sama, abaikan heartbeat kompetitor.
                if leader_id == self.node_id:
                    self.current_leader = self.node_id
                    self.failure_detector.reset_timer()
                    return web.json_response({"status": "ok"})

                return web.json_response({"status": "ditolak", "reason": "leader_konflik_term_sama"})

            if leader_term < self.term:
                self.failure_detector.reset_timer()
                return web.json_response({"status": "ditolak", "reason": "term_kedaluwarsa"})

        return web.json_response({"status": "pesan_tidak_dikenal"})

if __name__ == "__main__":
    node = LockManager()
    node.run()