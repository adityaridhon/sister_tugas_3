import hashlib
import bisect
import json
import redis
import asyncio
from aiohttp import web
from src.nodes.base_node import BaseNode

class ConsistentHashRing:
    def __init__(self, nodes, virtual_nodes=100):
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_keys = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node):
        for i in range(self.virtual_nodes):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            bisect.insort(self.sorted_keys, key)

    def get_node(self, item):
        if not self.ring: return None
        key = self._hash(item)
        idx = bisect.bisect(self.sorted_keys, key)
        if idx == len(self.sorted_keys): idx = 0
        return self.ring[self.sorted_keys[idx]]

    def _hash(self, key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)


class QueueNode(BaseNode):
    def __init__(self):
        super().__init__()
        self.redis_client = redis.Redis(host=self.redis_host, port=6379, decode_responses=True)
        
        all_nodes = [self.node_id] + [peer.split(':')[0] for peer in self.peers]
        self.ring = ConsistentHashRing(all_nodes)
        
        # Daftarkan API endpoint untuk Queue
        self.app.router.add_post('/queue/push', self.push_job)
        self.app.router.add_get('/queue/pop', self.pop_job)
        self.app.router.add_post('/queue/ack', self.ack_job)
        self.app.router.add_get('/queue/status', self.queue_status)

        # Jalankan fungsi recovery saat server baru nyala
        self.app.on_startup.append(self.recover_dead_jobs)

    async def recover_dead_jobs(self, app):
        queue_key = f"queue:{self.node_id}"
        processing_key = f"processing:{self.node_id}"
        
        recovered_count = 0
        while True:
            job_data = self.redis_client.rpop(processing_key)
            if not job_data: break
            
            self.redis_client.lpush(queue_key, job_data)
            recovered_count += 1
            
        if recovered_count > 0:
            print(f"[{self.node_id}] 🚑 RECOVERY SUKSES: Menyelamatkan {recovered_count} tugas yang nyangkut.")

    async def push_job(self, request):
        data = await request.json()
        job_id = data.get("job_id")
        payload = data.get("payload")
        
        target_node = self.ring.get_node(job_id)
        queue_key = f"queue:{target_node}"
        
        job_data = json.dumps({"job_id": job_id, "payload": payload})
        self.redis_client.rpush(queue_key, job_data)
        
        print(f"[{self.node_id}] 📥 Masuk antrean: Tugas {job_id} -> {target_node}")
        return web.json_response({"status": "sukses", "target_node": target_node})

    async def pop_job(self, request):
        queue_key = f"queue:{self.node_id}"
        processing_key = f"processing:{self.node_id}"
        
        job_data = self.redis_client.lpop(queue_key)
        
        if job_data:
            self.redis_client.rpush(processing_key, job_data)
            job = json.loads(job_data)
            print(f"[{self.node_id}] 🚀 Mulai memproses: {job['job_id']} (Menunggu ACK...)")
            return web.json_response({"status": "sukses", "job": job, "raw_data": job_data})
            
        return web.json_response({"status": "kosong", "pesan": "Tidak ada tugas."})

    async def ack_job(self, request):
        data = await request.json()
        raw_data = data.get("raw_data")
        
        processing_key = f"processing:{self.node_id}"
        removed = self.redis_client.lrem(processing_key, 1, raw_data)
        
        if removed > 0:
            print(f"[{self.node_id}] ✅ ACK DITERIMA: Tugas selesai dan dihapus.")
            return web.json_response({"status": "sukses", "pesan": "Tugas di-ACK"})
            
        return web.json_response({"status": "gagal", "pesan": "Tugas tidak ditemukan di processing"}, status=400)

    async def queue_status(self, request):
        queue_key = f"queue:{self.node_id}"
        proc_key = f"processing:{self.node_id}"
        return web.json_response({
            "node_id": self.node_id, 
            "antrean_menunggu": self.redis_client.llen(queue_key),
            "sedang_diproses": self.redis_client.llen(proc_key)
        })

if __name__ == "__main__":
    node = QueueNode()
    node.run()