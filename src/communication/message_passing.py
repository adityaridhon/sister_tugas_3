import aiohttp

class MessageCommunicator:
    def __init__(self, node_id):
        self.node_id = node_id

    async def send_message(self, peer_url, data):
        """Modul khusus untuk mengirim pesan ke node lain"""
        async with aiohttp.ClientSession() as session:
            try:
                url = f"http://{peer_url}/message"
                async with session.post(url, json=data, timeout=2) as resp:
                    return await resp.json()
            except Exception as e:
                print(f"[{self.node_id}] ❌ Gagal ngirim pesan ke {peer_url}: {str(e)}")
                return None