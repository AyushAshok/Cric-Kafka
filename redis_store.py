import redis
from datetime import datetime

redis_client = redis.Redis(host='localhost', port=6380, db=0)

def push_redis_database(topic,message):
    redis_client.rpush(topic,message)
    print(f"[Redis] Pushed topic(s) at {datetime.now().strftime('%d %m %Y %H:%M:%S')}")