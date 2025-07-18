import asyncio
import firebase_store
import redis_store
import argparse

topics = {}
shutdown_event = asyncio.Event()

parser = argparse.ArgumentParser()
parser.add_argument('--id', required=True, help="Unique broker ID")
parser.add_argument('--port', required=True, type=int, help="Port to bind the broker")
args = parser.parse_args()

broker_id = args.id
BROKER_PORT = args.port

async def handle_client(reader, writer):
    while True:
        data = await reader.readline()
        decoded = data.decode('utf-8').strip()
        if not decoded:
            break
        await store_data_topics(decoded)

    writer.close()
    await writer.wait_closed()

async def store_data_topics(data):
    if not data:
        print("Data not receiving")
        return

    parts = data.split('|', 3)
    if len(parts) < 4:
        print("Invalid data format")
        return

    topic = parts[1]
    message = parts[3]

    await store_redis_database(topic, message)

    if topic not in topics:
        topics[topic] = []

    topics[topic].append(message)

async def store_redis_database(topic, message):
    redis_store.push_redis_database(topic, message)

async def store_firebase_database():
    while not shutdown_event.is_set():
        await asyncio.sleep(60)
        firebase_store.push_firebase_database(topics.copy())

async def broker_heartbeat():
    while not shutdown_event.is_set():
        try:
            reader, writer = await asyncio.open_connection('localhost', 9999)
            msg = f"BROKER|{broker_id}|localhost|{BROKER_PORT}|RCBvsDC,RCBvsCSK\n"
            writer.write(msg.encode())
            await writer.drain()
            await reader.readline()
            writer.close()
            await writer.wait_closed()
            print(f"[Broker-{broker_id}] Heartbeat sent to Zookeeper.")
        except Exception as e:
            print(f"[Broker-{broker_id}] Failed to send heartbeat: {e}")
        await asyncio.sleep(5)

async def main():
    server = await asyncio.start_server(handle_client, 'localhost', BROKER_PORT)
    print(f"[Broker-{broker_id}] Server started on localhost:{BROKER_PORT}")

    server_task = asyncio.create_task(server.serve_forever())
    firebase_task = asyncio.create_task(store_firebase_database())
    heartbeat_task = asyncio.create_task(broker_heartbeat())

    try:
        await asyncio.gather(server_task, firebase_task, heartbeat_task)
    except KeyboardInterrupt:
        print(f"[Broker-{broker_id}] Shutdown requested.")
    finally:
        shutdown_event.set()
        for task in [server_task, firebase_task, heartbeat_task]:
            task.cancel()
        await asyncio.sleep(0.5)
        firebase_store.push_firebase_database(topics.copy())
        print(f"[Broker-{broker_id}] Gracefully stopped.")

if __name__ == '__main__':
    asyncio.run(main())
