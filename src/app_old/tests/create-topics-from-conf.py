import toml
import subprocess
import sys


with open('res/conf.toml', 'r') as f:
    confs = toml.loads(f.read())


for tpc in confs['Kafka']['Topics'].values():
    name = tpc['name']
    retention = tpc['retention']
    max_message_bytes = tpc['max_message_bytes']
    
    if name in ['rapidapi-booking-listings']:
        continue
    
    commands = [
        f'/opt/kafka/bin/kafka-topics.sh --bootstrap-server DPQDALPROD06:9092 --delete --topic {name}',
        f'/opt/kafka/bin/kafka-topics.sh --bootstrap-server DPQDALPROD06:9092 --create --topic {name}',
        f'/opt/kafka/bin/kafka-configs.sh --bootstrap-server DPQDALPROD06:9092 --entity-type topics --entity-name name --alter --add-config retention.ms={retention}',
        f'/opt/kafka/bin/kafka-configs.sh --bootstrap-server DPQDALPROD06:9092 --entity-type topics --entity-name name --alter --add-config max.message.bytes={max_message_bytes}'
    ]
    for command in commands:
        process = subprocess.run(command.split(' '))
        process.wait()