import redis
import json
import asyncio
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--id', required=True)
parser.add_argument('--port', required=True, type=int)
parser.add_argument('--topic',required=True)
args = parser.parse_args()

consumer_id = args.id
CONSUMER_PORT = args.port
consumer_topic=args.topic

redis_client = redis.Redis(host='localhost', port=6380, db=0)

async def consume():
    while True:
        try:
            score = await asyncio.to_thread(redis_client.blpop, consumer_topic, 10)
            if not score:
                continue

            _, message = score
            decoded_score = json.loads(message.decode())

            if decoded_score.get('event') == 'ERROR':
                continue

            print(f"[Consumer-{consumer_id}] Event: {decoded_score['event']} | Inning: {decoded_score['inning']} | Over: {decoded_score['over']}")
            redis_client.hset("offsets", f"{consumer_id}:consumer_topic", decoded_score['id'])

        except Exception as e:
            print(f"[Consumer-{consumer_id}] Error: {e}")
            continue

async def consumer_heartbeat():
    while True:
        try:
            reader, writer = await asyncio.open_connection('localhost', 9999)
            msg = f"CONSUMER|{consumer_id}|localhost|{CONSUMER_PORT}|{consumer_topic}\n"
            writer.write(msg.encode())
            await writer.drain()
            await reader.readline()  # ACK or MATCHED message
            writer.close()
            await writer.wait_closed()
            print(f"[Consumer-{consumer_id}] Heartbeat sent to Zookeeper.")
        except Exception as e:
            print(f"[Consumer-{consumer_id}] Heartbeat failed: {e}")
        await asyncio.sleep(5)

async def main():
    try:
        await asyncio.gather(consumer_heartbeat(), consume())
    except KeyboardInterrupt:
        print(f"[Consumer-{consumer_id}] Shutdown requested.")
    finally:
        print(f"[Consumer-{consumer_id}] Stopped.")

if __name__ == '__main__':
    asyncio.run(main())
