import requests

# Asumsi Docker sedang menyala dan Node 1 ada di port 8001
BASE_URL = "http://localhost:8001"

def test_cache_flow():
    print("Mulai Integration Test Cache...")
    
    # 1. Tes Set Cache
    payload = {"key": "test_integrasi", "value": "berhasil"}
    res_set = requests.post(f"{BASE_URL}/cache/set", json=payload)
    print("Response Set:", res_set.json())
    
    # 2. Tes Get Cache
    res_get = requests.get(f"{BASE_URL}/cache/get/test_integrasi")
    print("Response Get:", res_get.json())

    # 3. Tes Metrics
    res_metrics = requests.get(f"{BASE_URL}/cache/metrics")
    print("Response Metrics:", res_metrics.json())

if __name__ == "__main__":
    print("Pastikan docker-compose sedang berjalan (command: cache_node)!")
    test_cache_flow()
    print("Integration Test Selesai.")