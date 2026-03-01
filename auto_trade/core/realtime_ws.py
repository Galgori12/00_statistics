# core/realtime_ws.py (예시)
import asyncio
import json
import websockets

class KiwoomRealtimeClient:
    def __init__(self, token: str, uri: str):
        self.token = token
        self.uri = uri
        self.ws = None
        self.running = True

    async def connect(self):
        self.ws = await websockets.connect(self.uri)
        await self.ws.send(json.dumps({"trnm": "LOGIN", "token": self.token}))

    async def reg(self, grp_no: str, items: list[str], types: list[str], refresh: str = "1"):
        payload = {
            "trnm": "REG",
            "grp_no": grp_no,
            "refresh": refresh,
            "data": [{"item": items, "type": types}]
        }
        await self.ws.send(json.dumps(payload))

    async def loop(self, on_real):
        """
        on_real: callable(dict) -> None
        """
        while self.running:
            msg = await self.ws.recv()
            data = json.loads(msg)

            trnm = data.get("trnm")
            if trnm == "PING":
                await self.ws.send(json.dumps(data))
            elif trnm == "REAL":
                on_real(data)
            elif trnm == "LOGIN":
                if data.get("return_code") != 0:
                    raise RuntimeError(f"LOGIN 실패: {data.get('return_msg')}")