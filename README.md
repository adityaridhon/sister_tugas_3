# Distributed Sync System

Sistem sinkronisasi terdistribusi berbasis Python + Redis yang terdiri dari 3 layanan utama:

- Cache layer (`cache1-3`, port `8001-8003`)
- Queue layer (`queue1-3`, port `8004-8006`)
- Lock layer dengan Raft (`lock1-3`, port `8007-8009`)

Dokumentasi API berjalan di Swagger UI pada port `8080`, dan performance UI Locust di port `8089`.

## Prasyarat

- Git
- Docker Desktop (Windows/Mac) atau Docker Engine (Linux)
- Docker Compose v2 (`docker compose`) atau `docker-compose`

## 1. Clone Repository

```bash
git clone https://github.com/adityaridhon/sister_tugas_3
cd sister_tugas_3
```

## 2. Siapkan Environment File

Nilai penting default:

- Redis: `6379`
- Cache nodes: `8001-8003`
- Queue nodes: `8004-8006`
- Lock nodes: `8007-8009`

## 3. Build dan Jalankan dengan Docker Compose

```bash
docker compose --env-file .env -f docker/docker-compose.yml up --build -d
```

## 4. Verifikasi Container Berjalan

```bash
docker compose -f docker/docker-compose.yml ps
```

Service yang diharapkan muncul:

- `redis`
- `cache1`, `cache2`, `cache3`
- `queue1`, `queue2`, `queue3`
- `lock1`, `lock2`, `lock3`
- `swagger-ui`
- `locust`

## 5. Akses Endpoint Penting

- Swagger UI: `http://localhost:8080`
- Locust UI: `http://localhost:8089`

## 6. Lihat Log Service

Contoh melihat log lock cluster:

```bash
docker compose -f docker/docker-compose.yml logs -f lock1 lock2 lock3
```

Atau semua service:

```bash
docker compose -f docker/docker-compose.yml logs -f
```

## 7. Stop dan Bersihkan

Stop semua container:

```bash
docker compose -f docker/docker-compose.yml down
```

Stop + hapus volume:

```bash
docker compose -f docker/docker-compose.yml down -v
```

## Catatan

- Untuk testing failover lock (Raft), matikan leader lock (`lock1`/`lock2`/`lock3`) dan cek node lain akan melakukan election ulang.
- `api_spec.yaml` dipasang ke container Swagger melalui volume `../docs:/docs`.
