import random
from datetime import datetime
import asyncio
import json


class CricketGenerator:
    def __init__(self,port=8888,host='localhost'):
        self.port=port
        self.host=host
        self.connected=False
        self.reader=None
        self.writer=None

    async def connect_to_port(self):
        try:
            self.reader,self.writer= await asyncio.open_connection(self.host,self.port)
            self.connected=True
            print(f'Connection to {self.host}:{self.port} Successfull!!')
        except Exception as e:
            print(e)
            self.connected=False

    async def send_data(self,topic,message):
        if not self.connected:
            print("Not Connected to port ",self.port)
            return False
        
        
        try:
            # Create message protocol: TOPIC_LENGTH|TOPIC|MESSAGE_LENGTH|MESSAGE
            topic_bytes = topic.encode('utf-8')
            message_bytes = json.dumps(message).encode('utf-8')
            
            # Create protocol header
            header = f"{len(topic_bytes)}|{topic}|{len(message_bytes)}|".encode('utf-8')
            full_message = header + message_bytes + b"\n"
            
            # Send to broker
            self.writer.write(full_message)
            await self.writer.drain()
            return True
            
        except Exception as e:
            print(f"Failed to send message: {e}")
            return False

        


    async def disconnect(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        self.connected=False
        print("Disconnected Successfully")


    async def simulate_match(self):
        ball=1
        id=1
        innings=1
        total_ball=1

        await self.connect_to_port()

        if not self.connected:
            print("Cannot connect to the port")
            return
        

        try:
            while(total_ball<=240):  
                c = random.choice(["W","ERROR", "WD", "LB","ERROR", "B", "NB","ERROR", "1", "2","ERROR", "3", "4", "6","ERROR"])
                t = random.choice(["RCBvsDC","RCBvsCSK"])

                if c in ["4", "6"]:
                    category = "boundary"
                elif c in ["1", "2", "3"]:
                    category = "run"
                elif c in ["LB", "B"]:
                    category = "extra"
                elif c in ["NB","WD"]:
                    ball-=1
                    total_ball-=1
                    category="extra-ball"
                elif c == "W":
                    category = "wicket"
                elif c=="ERROR":
                    category="error"
                    ball-=1
                    total_ball-=1
                else:
                    category = "unknown"

                if(ball==121):
                    innings=2
                    ball=1

                if(category=="error"):
                    info={
                        "id":id,
                        "event":c,
                        "timestamp": datetime.now().strftime("%d %m %Y %H:%M:%S")
                    }
                else:
                    info = {
                        "id": id,
                        "event": c,
                        "inning":innings,
                        "category": category,
                        "over": f'{ball // 6}.{ball % 6}',
                        "timestamp": datetime.now().strftime("%d %m %Y %H:%M:%S")
                    }

                success = await self.send_data(t, info)

                if success:
                    print("[Producer] Sent match Info", info)
                else:
                    print("[Producer] Failed to send Info", info)

                ball += 1
                total_ball+=1
                id += 1

                await asyncio.sleep(5)

        except KeyboardInterrupt:
            print("KeyBoard Interrupted")
        except Exception as e:
            print("Error: ",e)
        finally:
            await self.disconnect()

async def main():
    producer=CricketGenerator()
    await producer.simulate_match()

if __name__=='__main__':
    asyncio.run(main())
