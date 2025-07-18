# Cric-Kafka
A cricket streaming service able to handle multiple producers,consumers and brokers

To run:
1st Terminal- Run Zookeeper using python zookeepr.py
2nd Terminal- Run Broker using python broker.py --id (any id like broker1) --port PORT_NO.
3rd Terminal -Run Producer using python producer.py --id (any id like producer1) --port PORT_NO.
4th Terminal -Run Consumer using python consumer.py --id (any id like consumer1) --port PORT_NO --topic (any topic that the producer is producing like RCBvsDC).

On other terminals say 5th,6th,7th,etc you can run multiple producers, consumers, and brokers with different id and port nos.
