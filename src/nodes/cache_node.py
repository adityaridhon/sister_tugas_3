import json
import redis
import asyncio
from collections import OrderedDict
from aiohttp import web
from src.nodes.base_node import BaseNode

class CacheNode(BaseNode):
    def __init__(self):
        super().__init__()
        self.redis_client = redis.Redis(host=self.redis_host, port=6379, decode_responses=True)
        
        self.MAX_CACHE_SIZE = 3 
        self.local_cache = OrderedDict() 
        self.metrics = {"hits": 0, "misses": 0, "evictions": 0}
        
        self.app.router.add_post('/cache/set', self.set_cache)
        self.app.router.add_get('/cache/get/{key}', self.get_cache)
        self.app.router.add_get('/cache/metrics', self.get_metrics)
        self.app.router.add_post('/mesi/bus', self.handle_mesi_bus)

    def _evict_lru_if_needed(self):
        if len(self.local_cache) >= self.MAX_CACHE_SIZE:
            oldest_key, oldest_item = self.local_cache.popitem(last=False)
            self.metrics["evictions"] += 1
            print(f"[{self.node_id}] ♻️ LRU EVICTION: Membuang '{oldest_key}'.")
            
            if oldest_item["state"] == "M":
                print(f"[{self.node_id}] 💾 WRITE-BACK: Menyimpan '{oldest_key}' ke Redis.")
                self.redis_client.set(oldest_key, json.dumps(oldest_item["value"]))

    async def get_cache(self, request):
        key = request.match_info.get('key')
        
        if key in self.local_cache and self.local_cache[key]["state"] != "I":
            self.metrics["hits"] += 1
            self.local_cache.move_to_end(key)
            state = self.local_cache[key]["state"]
            print(f"[{self.node_id}] 🎯 CACHE HIT! Membaca '{key}' (State: {state})")
            return web.json_response({"status": "sukses", "sumber": "L1_Local", "state": state, "data": self.local_cache[key]["value"]})
            
        self.metrics["misses"] += 1
        print(f"[{self.node_id}] ❌ CACHE MISS untuk '{key}'. Bertanya ke node lain...")
        
        peer_has_it = await self.broadcast_mesi("BusRd", key)
        
        cached_data = self.redis_client.get(key)
        val = json.loads(cached_data) if cached_data else None
        
        if val is None:
            return web.json_response({"status": "gagal", "pesan": "Data tidak ada di database"}, status=404)

        self._evict_lru_if_needed()
        
        new_state = "S" if peer_has_it else "E"
        self.local_cache[key] = {"value": val, "state": new_state}
        self.local_cache.move_to_end(key)
        
        print(f"[{self.node_id}] 📥 Load dari Redis. State MESI '{key}': {new_state}")
        return web.json_response({"status": "sukses", "sumber": "L2_Redis", "state": new_state, "data": val})

    async def set_cache(self, request):
        data = await request.json()
        key = data.get("key")
        value = data.get("value")
        
        is_hit = key in self.local_cache and self.local_cache[key]["state"] != "I"
        current_state = self.local_cache[key]["state"] if is_hit else "I"
        
        if current_state in ["S", "I"]:
            print(f"[{self.node_id}] 📢 BROADCAST INVALIDATE: Menyuruh node lain menghapus '{key}'...")
            await self.broadcast_mesi("Invalidate", key)
            
        self._evict_lru_if_needed()
        
        self.local_cache[key] = {"value": value, "state": "M"}
        self.local_cache.move_to_end(key)
        self.redis_client.set(key, json.dumps(value))
        
        print(f"[{self.node_id}] 📝 WRITE SUKSES. State '{key}': M (Modified)")
        return web.json_response({"status": "sukses", "state": "M", "pesan": f"Cache diupdate"})

    async def get_metrics(self, request):
        return web.json_response({
            "node_id": self.node_id,
            "metrics": self.metrics,
            "current_cache_size": len(self.local_cache),
            "cache_keys": list(self.local_cache.keys())
        })

    async def broadcast_mesi(self, action, key):
        tasks = []
        for peer in self.peers:
            url = f"http://{peer}/mesi/bus"
            # Sudah kuperbaiki jadi langsung manggil fungsinya dengan benar
            tasks.append(self.send_custom_message(url, {"action": action, "key": key}))
        responses = await asyncio.gather(*tasks)
        return any(resp and resp.get("status") == "HasData" for resp in responses)

    async def handle_mesi_bus(self, request):
        data = await request.json()
        action = data.get("action")
        key = data.get("key")
        
        if key not in self.local_cache or self.local_cache[key]["state"] == "I":
            return web.json_response({"status": "NoData"})
            
        current_state = self.local_cache[key]["state"]
        
        if action == "BusRd":
            if current_state == "M":
                self.redis_client.set(key, json.dumps(self.local_cache[key]["value"]))
            self.local_cache[key]["state"] = "S"
            print(f"[{self.node_id}] 📉 MESI BUS: Seseorang membaca '{key}'. State jadi S (Shared).")
            return web.json_response({"status": "HasData"})
            
        elif action == "Invalidate":
            del self.local_cache[key]
            print(f"[{self.node_id}] 🗑️ MESI BUS: INVALIDATE diterima. Menghapus '{key}'.")
            return web.json_response({"status": "Ack"})

    async def send_custom_message(self, url, data):
        import aiohttp
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=data, timeout=2) as resp:
                    return await resp.json()
            except:
                return None

if __name__ == "__main__":
    node = CacheNode()
    node.run()