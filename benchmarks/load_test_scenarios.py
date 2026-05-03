import asyncio
import aiohttp
import time

# Konfigurasi Target
NODES_ALL = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
NODE_SINGLE = ["http://localhost:8001"]

async def fetch(session, url, method="GET", payload=None):
    start_time = time.time()
    try:
        if method == "GET":
            async with session.get(url) as response:
                await response.text()
        else:
            async with session.post(url, json=payload) as response:
                await response.text()
        return time.time() - start_time, True
    except Exception:
        # Jika koneksi gagal (server down/kewalahan)
        return time.time() - start_time, False

async def run_scenario(name, requests_count, method, endpoint, target_nodes, is_write=False, silent=False):
    if not silent:
        print(f"--- Menjalankan Skenario: {name} ({requests_count} Requests) ---")
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(requests_count):
            # Round-robin ke node yang tersedia
            node = target_nodes[i % len(target_nodes)]
            url = f"{node}{endpoint}"
            
            payload = None
            if is_write:
                if "queue" in endpoint:
                    payload = {"job_id": f"job_bench_{i}", "payload": "tes_beban"}
                else:
                    payload = {"key": f"bench_{i}", "value": "data_tes"}
            
            tasks.append(fetch(session, url, method, payload))
        
        start_total = time.time()
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_total
        
    success_count = sum(1 for r in results if r[1])
    latencies = [r[0] for r in results if r[1]]
    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    throughput = success_count / total_time if total_time > 0 else 0
    
    if not silent:
        print(f"Sukses: {success_count}/{requests_count}")
        print(f"Total Waktu: {total_time:.2f} detik")
        print(f"Throughput: {throughput:.2f} req/sec")
        print(f"Rata-rata Latency: {avg_latency:.4f} detik\n")
        
    return throughput, avg_latency, total_time

async def main():
    
    # --- SINGLE VS DISTRIBUTED ---
    print("Menjalankan Tes Perbandingan (Mohon tunggu sebentar)...")
    req_compare = 500
    
    # Tes Single
    tp_single, lat_single, time_single = await run_scenario("Single", req_compare, "GET", "/cache/get/bench_key", NODE_SINGLE, silent=True)
    await asyncio.sleep(1) #
    
    # Tes Distributed
    tp_dist, lat_dist, time_dist = await run_scenario("Distributed", req_compare, "GET", "/cache/get/bench_key", NODES_ALL, silent=True)
    
    # Cetak Tabel Perbandingan
    print("SINGLE vs DISTRIBUTED (Skenario: Read Cache 500 Req)")
    print("-" * 65)
    print(f"{'Metrik':<20} | {'Single Node':<18} | {'Distributed (3 Nodes)'}")
    print("-" * 65)
    print(f"{'Waktu Selesai':<20} | {time_single:.2f} detik{'':<7} | {time_dist:.2f} detik")
    print(f"{'Throughput (RPS)':<20} | {tp_single:.2f} req/s{'':<5} | {tp_dist:.2f} req/s")
    print(f"{'Rata-rata Latency':<20} | {lat_single*1000:.2f} ms{'':<9} | {lat_dist*1000:.2f} ms")
    print("-" * 65)
    
    if tp_dist > tp_single:
        peningkatan = ((tp_dist - tp_single) / tp_single) * 100
        print(f" Arsitektur Distributed lebih cepat {peningkatan:.1f}% menangani beban!\n")
    else:
         print("Performa relatif seimbang pada beban ini.\n")
         
    await asyncio.sleep(2)
    
    # --- FULL SYSTEM BENCHMARK ---
    print("FULL SYSTEM BENCHMARK SCENARIOS")
    
    await run_scenario("1. Cache Read-Heavy (Distributed)", 500, "GET", "/cache/get/data_test", NODES_ALL)
    await run_scenario("2. Cache Write-Heavy (Distributed)", 200, "POST", "/cache/set", NODES_ALL, is_write=True)
    await run_scenario("3. Queue Push Burst (Distributed)", 300, "POST", "/queue/push", NODES_ALL, is_write=True)
    
    print("Semua tes selesai!")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())