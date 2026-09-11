"""Relay example from the reliomq Basic Guide.

Run this file before sender.py:

    python relay.py

Press Ctrl+C to stop it cleanly.
"""

import threading

from reliomq import Relay, RelayConfig


config = RelayConfig(
    # MQTT broker ที่รับ envelope จาก Sender
    # localhost หมายถึง broker อยู่บนเครื่องเดียวกับโปรแกรมนี้
    source_host="localhost",

    # MQTT broker ปลายทางที่ Relay จะส่ง application message ไปให้
    # ตัวอย่างใช้ broker เครื่องเดียวกัน จึงเป็น localhost เหมือนกัน
    destination_host="localhost",

    # Port ของ source broker ค่า MQTT ปกติคือ 1883
    source_port=1883,

    # Port ของ destination broker ค่า MQTT ปกติคือ 1883
    destination_port=1883,

    # Client ID ของ Relay ฝั่งรับข้อความ
    # connection นี้เชื่อมกับ local/source broker เพื่อรับ envelope ที่ Sender ส่งมา
    # นี่คือ ID ของ Relay ไม่ใช่ client_id ของ Sender และต้องไม่ใช้ชื่อซ้ำกัน
    source_client_id="demo-relay-source",

    # Client ID ของ Relay ฝั่งส่งข้อความ
    # connection นี้เชื่อมกับ remote/destination broker เพื่อส่งข้อมูลไปปลายทางจริง
    # เป็นคนละ connection กับฝั่งรับ จึงต้องใช้ชื่อต่างจาก source_client_id
    destination_client_id="demo-relay-destination",

    # Topic ภายในที่ Relay ใช้รับ envelope จาก Sender
    # ค่านี้ต้องตรงกับ relay_topic ใน sender.py
    relay_topic="reliomq/messages",

    # Topic ภายในที่ Relay ใช้ส่ง DeliveryAck กลับไปหา Sender
    # ค่านี้ต้องตรงกับ delivery_ack_topic ใน sender.py
    delivery_ack_topic="reliomq/acks",

    # เปิด log ระดับ INFO เพื่อให้เห็นการ connect, forward และส่ง ACK
    log_level="INFO",
)

# สร้าง Relay จากค่าที่กำหนดด้านบน
relay = Relay(config)

try:
    # เชื่อมต่อทั้ง source broker และ destination broker
    relay.connect()

    # เปิดโปรแกรมค้างไว้เพื่อรอรับและส่งต่อข้อความ
    threading.Event().wait()
except KeyboardInterrupt:
    # ผู้ใช้กด Ctrl+C เพื่อหยุดโปรแกรม จึงออกจากการรอโดยไม่แสดง error
    pass
finally:
    # ปิด connection และ worker ของ Relay อย่างเรียบร้อยเสมอ
    relay.disconnect()
