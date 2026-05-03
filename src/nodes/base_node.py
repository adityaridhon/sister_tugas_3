import os
import socket
from aiohttp import web
from dotenv import load_dotenv
from src.communication.message_passing import MessageCommunicator

load_dotenv()

@web.middleware
async def cors_middleware(request, handler):
    if request.method == 'OPTIONS':
        response = web.Response()
    else:
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = exc
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

class BaseNode:
    def __init__(self):
        self.node_id = os.getenv('NODE_ID') or socket.gethostname()
        self.port = int(os.getenv('PORT', 8080))
        
        raw_peers = os.getenv('PEERS', '')
        self.peers = [p.strip() for p in raw_peers.split(',') if p.strip()]
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        
        self.app = web.Application(middlewares=[cors_middleware])
        self.setup_routes()
        
        self.communicator = MessageCommunicator(self.node_id)
        
        print(f"[{self.node_id}] Server menyala. Daftar teman (Peers): {self.peers}")

    def setup_routes(self):
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_post('/message', self.handle_message)

    async def health_check(self, request):
        return web.json_response({"status": "ok", "node": self.node_id})

    async def handle_message(self, request):
        return web.json_response({"status": "diterima_base"})

    async def send_message(self, peer_url, data):
        return await self.communicator.send_message(peer_url, data)

    def run(self):
        web.run_app(self.app, host='0.0.0.0', port=self.port)