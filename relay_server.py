import asyncio
import json
import os
import random
import websockets

# rooms: { "12345678": { "host": websocket, "clients": {websocket: player_id, ...}, "next_id": 2 } }
rooms = {}


def generate_code():
    while True:
        code = "".join(str(random.randint(0, 9)) for _ in range(8))
        if code not in rooms:
            return code


async def broadcast(room, message, exclude=None):
    data = json.dumps(message)
    targets = [room["host"]] + list(room["clients"].keys())
    for ws in targets:
        if ws is exclude:
            continue
        try:
            await ws.send(data)
        except Exception:
            pass


async def handle_client(websocket):
    room_code = None
    role = None  # "host" or "client"

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")

            if msg_type == "host":
                code = generate_code()
                rooms[code] = {"host": websocket, "clients": {}, "next_id": 2}
                room_code = code
                role = "host"
                await websocket.send(json.dumps({"type": "hosted", "code": code, "playerId": 1}))
                print(f"[Room {code}] created")

            elif msg_type == "join":
                code = msg.get("code", "")
                room = rooms.get(code)
                if not room:
                    await websocket.send(json.dumps({"type": "error", "message": "Code not found"}))
                    continue

                player_id = room["next_id"]
                room["next_id"] += 1
                room["clients"][websocket] = player_id
                room_code = code
                role = "client"

                await websocket.send(json.dumps({"type": "joined", "code": code, "playerId": player_id}))
                await broadcast(room, {"type": "player_joined", "playerId": player_id}, exclude=websocket)
                print(f"[Room {code}] player {player_id} joined")

            elif msg_type == "state" or msg_type == "event":
                room = rooms.get(room_code)
                if room:
                    await broadcast(room, msg, exclude=websocket)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if room_code and room_code in rooms:
            room = rooms[room_code]
            if role == "host":
                await broadcast(room, {"type": "host_left"})
                del rooms[room_code]
                print(f"[Room {room_code}] closed (host left)")
            elif websocket in room["clients"]:
                player_id = room["clients"].pop(websocket)
                await broadcast(room, {"type": "player_left", "playerId": player_id})
                print(f"[Room {room_code}] player {player_id} left")


async def main():
    port = int(os.environ.get("PORT", 10000))
    print(f"Relay server starting on 0.0.0.0:{port}")
    async with websockets.serve(handle_client, "0.0.0.0", port):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
