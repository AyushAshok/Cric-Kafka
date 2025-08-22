# Cric-Kafka
A cricket streaming service able to handle multiple producers,consumers and brokers

To run:<br>
1st Terminal- Run Zookeeper using python zookeepr.py --port PORT_NO. <br>
2nd Terminal- Run Broker using python broker.py --id (any id like broker1) --port PORT_NO. (The PORT_NO you give here will be the port no of the producer it will listen to)<br>
3rd Terminal -Run Producer using python producer.py --id (any id like producer1) --port PORT_NO. <br>
4th Terminal -Run Consumer using python consumer.py --id (any id like consumer1) --port PORT_NO --topic (any topic that the producer is producing like RCBvsDC). <br> <br>

On other terminals say 5th,6th,7th,etc you can run multiple producers, consumers, and brokers with different id and port nos. <br>

