#kafka python api
from kafka import KafkaProducer

if __name__ == '__main__':

    #1 create kafka producer
    KafkaProducer = KafkaProducer(
        bootstrap_servers = ['node1:9092','node2:9092','node3:9092']
    )

    #2 send 
    for i in range(0,100):
        # Synchronization-based sending mode
        metadata = KafkaProducer.send(topic='test01',value=f'{i}'.encode('UTF-8')).get() #value must be bytes
        print(record_metadata.topic) # show which topic you have sent
        print(record_metadata.partition) # which partition you have sent
        print(record_metadata.offset) # Offset for sending messages
        # Asynchronous based sending mode
        #KafkaProducer.send(topic='test01',value=f'{i}'.encode('UTF-8'))
    #3 close source
    KafkaProducer.close()







# consumer
from kafka import KafkaConsumer


if __name__ == '__main__':

    #1 create kafka producer
    consumer = KafkaConsumer(
        'test01',
        bootstrap_servers = ['node1:9092','node2:9092','node3:9092'],
        group_id = 'g_1'
    )

    #2 get the message
    for message in consumer:
        topic = message.topic
        partition = message.partition
        offset = message.offset
        key = message.key
        value = message.value.decode('UTF-8')
        print(f"from {topic}'s {partition}, get the message {key}-{value} offset is {offset}")


# kafka storage
