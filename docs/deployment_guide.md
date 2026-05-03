# Deployment & Troubleshooting Guide

## Persyaratan Sistem

- Docker & Docker Compose
- Python 3.10+ (Opsional jika run tanpa Docker)

## Cara Deployment

1. Atur konfigurasi port dan peers di dalam file `.env`.
2. Buka terminal di root direktori.
3. Jalankan perintah: `docker-compose --env-file .env -f docker/docker-compose.yml up --build -d`
4. Akses Swagger UI di `http://localhost:8080`.

## Troubleshooting

- **Node Crash (Error 404/Connection Refused):** Pastikan library di `requirements.txt` lengkap (seperti `redis`, `aiohttp`) lalu lakukan rebuild dengan `docker-compose down` dan `docker-compose up --build`.
- **CORS Error di Swagger:** Pastikan request ditembakkan ke port yang aktif sesuai di env, dan pastikan middleware CORS di `base_node.py` berjalan.
