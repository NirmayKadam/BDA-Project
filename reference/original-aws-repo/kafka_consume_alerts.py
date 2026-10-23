from kafka import KafkaConsumer
import sys
import json
import boto3

consumer=KafkaConsumer("Alerts-topic",bootstrap_servers=["localhost:9092"],auto_offset_reset='earliest',value_deserializer=lambda m: json.loads(m.decode("utf-8")))

sns = boto3.client('sns',region_name='us-east-1')

for message in consumer:
	alert="Alert Details -"
	alert=alert+"\nAlert Message:"+str(message[6]['alert_message'])
	alert=alert+"\nPatient Name:"+str(message[6]['patientname'])
	alert=alert+"\nAdmitted Ward:"+str(message[6]['admitted_ward'])
	alert=alert+"\nAge:"+str(message[6]['age'])
	alert=alert+"\nBP:"+str(message[6]['bp'])
	alert=alert+"\nHeart Beat:"+str(message[6]['heartBeat'])
	response = sns.publish(
                TopicArn='arn:aws:sns:us-east-1:864328032829:Doctor-Queue',
                Message=str(alert)
    )


# Terminate the script
sys.exit()
