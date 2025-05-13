from confluent_kafka import Consumer, KafkaException

# Thiết lập Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9093',
    'group.id': 'test-consumer-group',
    'auto.offset.reset': 'latest'  # Lấy dữ liệu từ đầu
}

# Khởi tạo Consumer
consumer = Consumer(conf)



# Đăng ký vào topic
consumer.subscribe(['test-topic'])
# Poll một message
msg = consumer.poll(timeout=10.0)  # timeout sau 10 giây nếu không có message

if msg is None:
    print("Không nhận được message nào.")
elif msg.error():
    print(f"Lỗi khi nhận message: {msg.error()}")
else:
    print(f"Nhận được 1 message từ topic {msg.topic()}:")
    print(f"Key: {msg.key()}")
    print(f"Value: {msg.value()}")
    print(f"Partition: {msg.partition()}, Offset: {msg.offset()}")

# Đóng consumer
consumer.close()