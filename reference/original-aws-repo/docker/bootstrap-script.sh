#!/bin/bash

grep -q '"isMaster": true' /mnt/var/lib/info/instance.json || { 
  echo "Not running on master node, installing boto3 and python3.9 only"
  sudo yum install libffi-devel -y
  sudo wget https://www.python.org/ftp/python/3.9.0/Python-3.9.0.tgz   
  sudo tar -zxvf Python-3.9.0.tgz
  cd Python-3.9.0
  sudo ./configure --enable-optimizations
  sudo make altinstall
  python3.9 -m pip install --upgrade awscli --user
  sudo ln -sf /usr/local/bin/python3.9 /usr/bin/python3 
  sudo python3 -m pip install boto3
  exit 0 
}

#upgrade python
sudo yum install libffi-devel -y
sudo wget https://www.python.org/ftp/python/3.9.0/Python-3.9.0.tgz   
sudo tar -zxvf Python-3.9.0.tgz
cd Python-3.9.0
sudo ./configure --enable-optimizations
sudo make altinstall
python3.9 -m pip install --upgrade awscli --user
sudo ln -sf /usr/local/bin/python3.9 /usr/bin/python3

# Download and extract Kafka
cd ..
sudo wget https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz
sudo tar -xvf kafka_2.12-3.6.0.tgz

cd kafka_2.12-3.6.0
sudo bin/zookeeper-server-start.sh config/zookeeper.properties

#Wait for Zookeeper to start (change the port accordingly)
# echo "Waiting for Zookeeper to start..."
# while ! nc -z localhost 2181; do
#   sleep 1
# done
# echo "Zookeeper is up and running!"

#Start Kafka server
sudo bin/kafka-server-start.sh config/server.properties

#Wait for Kafka server to start (change the port accordingly)
# echo "Waiting for Kafka server to start..."
# while ! nc -z localhost 9092; do
#   sleep 1
# done
# echo "Kafka server is up and running!"

# Create Kafka topic
sudo bin/kafka-topics.sh --create --topic patientvitals --bootstrap-server localhost:9092
cd ..
# Copy producer script from S3
sudo aws s3 cp s3://patient-vital-monitoring-system/producer.py .

# Install required Python packages
sudo -E python3 -m pip install kafka-python scipy boto3

# Run the producer script
sudo -E python3 producer.py
