import asyncio
import time

brokers={}
consumers={}
topics_map={}
TIMEOUT=30

async def heartbeat_handle(reader,writer):
    try:
        data=await reader.readline()
        msg=data.decode().strip()

        if not msg:
            return
        
        parts=msg.split('|')
        if len(parts)!=5:
            writer.write(b"ERROR|Invalid message format\n")
            await writer.drain()
            return
        else:
            type,_id,host,port,topic_str=parts
            topics=topic_str.split(',') if topic_str else []

            if type=='BROKER':
                brokers[_id] = {'host': host, 'port': port, 'topics':topics,'last_seen': time.time()}
                for topic in topics:
                    topics_map.setdefault(topic, set()).add(_id)
                print(f"[Zookeeper] Registered broker: {_id} at {host}:{port}")

            elif type=='CONSUMER':
                consumers[_id] = {'host': host, 'port': port, 'topics':topics, 'last_seen': time.time()}
                print(f"[Zookeeper] Registered consumer: {_id} at {host}:{port}")
                matched = find_brokers_for_consumer(_id)
                print(f"[ZK] Matched Brokers for {_id}: {matched}")
            else:
                print(f"[Zookeeper] Unknown registration type: {type}")
            
            writer.write(b"ACK\n")
            await writer.drain()



    except Exception as e:
        print(f"[Zookeeper] Error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


def find_brokers_for_consumer(consumer_id):
    consumer_topics = set(consumers[consumer_id]['topics'])
    matched_brokers = set()
    for topic in consumer_topics:
        matched_brokers.update(topics_map.get(topic, []))
    return list(matched_brokers)


async def cleanup_down_nodes():
    while True:
        now=time.time()

        for broker_id in list(brokers):
            if now-brokers[broker_id]['last_seen']>TIMEOUT:
                print(f"[Zookeeper] Broker {broker_id} timed out.")
                for topic in brokers[broker_id]['topics']:
                    topics_map[topic].discard(broker_id)
                    if not topics_map[topic]:
                        del topics_map[topic]
                del brokers[broker_id]

        for consumer_id in list(consumers):
            if now - consumers[consumer_id]['last_seen'] > TIMEOUT:
                print(f"[Zookeeper] Consumer {consumer_id} timed out.")
                del consumers[consumer_id]

        await asyncio.sleep(5)


async def main():
    server = await asyncio.start_server(heartbeat_handle, 'localhost', 9999)
    print("[Zookeeper] Server started on port 9999")

    try:
        async with server:
            await asyncio.gather(server.serve_forever(),cleanup_down_nodes())
    except KeyboardInterrupt:
        print("[Zookeeper] Stopped")
    finally:
        pass

if __name__ == '__main__':
    asyncio.run(main())