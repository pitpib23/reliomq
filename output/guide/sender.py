"""Looping Sender example from the reliomq Basic Guide.

Start relay.py and a destination subscriber first, then run:

    python sender.py

This example sends simulated sensor data every two seconds. Press Ctrl+C to
stop it cleanly.
"""

import random
import time

from reliomq import Sender, SenderConfig


config = SenderConfig(
    # MQTT broker ฝั่ง source ที่ Sender เชื่อมต่อเพื่อส่ง envelope ให้ Relay
    host="localhost",

    # Port ของ source MQTT broker ค่า MQTT ปกติคือ 1883
    port=1883,

    # ชื่อ MQTT client ของ Sensor ต้องไม่ซ้ำกับ client ที่ online พร้อมกัน
    client_id="demo-sensor-01",

    # โฟลเดอร์ Outbox สำหรับข้อความที่ต้องเก็บลง disk
    # Sender แต่ละ process ต้องใช้ path ของตัวเองและห้ามใช้ร่วมกัน
    outbox_path="./outbox/sensor01",

    # Topic ภายในที่ Sender ใช้ส่ง envelope ไปให้ Relay
    # ค่านี้ต้องตรงกับ relay_topic ใน relay.py
    relay_topic="reliomq/messages",

    # Topic ภายในที่ Sender รอรับ DeliveryAck จาก Relay
    # ค่านี้ต้องตรงกับ delivery_ack_topic ใน relay.py
    delivery_ack_topic="reliomq/acks",

    # เปิด log ระดับ INFO เพื่อดูสถานะ connection, publish, retry และ ACK
    log_level="INFO",
)

# ไม่ระบุ mode จึงใช้ FastMode ซึ่งเป็นค่าเริ่มต้นของ reliomq 0.6.2
# with จะ connect ตอนเริ่ม และ disconnect อย่างเรียบร้อยเมื่อออกจาก block
with Sender(config) as sender:
    try:
        # LOOP: สร้างและส่งข้อมูลชุดใหม่ต่อเนื่องจนกว่าจะกด Ctrl+C
        while True:
            data = {
                # สุ่มอุณหภูมิระหว่าง 24 ถึง 35 แล้วปัดเป็นทศนิยม 2 ตำแหน่ง
                "temperature": round(random.uniform(24, 35), 2),

                # สุ่มความชื้นระหว่าง 45 ถึง 75 แล้วปัดเป็นทศนิยม 2 ตำแหน่ง
                "humidity": round(random.uniform(45, 75), 2),
            }

            message_id = sender.publish(
                # Application topic ที่ subscriber หรือระบบปลายทางต้องติดตาม
                "factory/demo/sensor01",

                # Payload ที่ต้องการส่ง ในตัวอย่างคือ dictionary ชื่อ data
                data,
            )

            # message_id ใช้ติดตามข้อความและใช้ deduplicate ที่ปลายทางได้
            print("accepted:", message_id, data)

            # จำนวนข้อความที่ยังรอให้ workflow จบด้วย DeliveryAck
            print("pending:", sender.pending_count())

            # รอ 2 วินาทีก่อนเริ่มรอบถัดไป จึงส่งประมาณ 1 ครั้งต่อ 2 วินาที
            time.sleep(2)
    except KeyboardInterrupt:
        # ผู้ใช้กด Ctrl+C เพื่อหยุด loop โดยไม่แสดง traceback
        print("sensor stopped")
