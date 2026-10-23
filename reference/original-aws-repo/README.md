# Real-Time Patient Monitoring System

This project report details the development of a real-time patient monitoring system aimed at revolutionizing healthcare by enabling continuous, remote monitoring of crucial vital signs. Fueled by advancements in the Internet of Medical Things (IoMT), this system empowers medical professionals to take swift and informed action, ultimately improving patient outcomes.

## Problem Statement
The healthcare landscape is rapidly evolving, demanding efficient and proactive solutions for patient monitoring. This project addresses the critical gap in real-time data acquisition and automated alert generation, ensuring timely intervention and improved patient care.

## Key Challenges:

1. Guaranteeing the reliability and accuracy of real-time data streams from IoMT devices.
2. Developing an intelligent alert mechanism for immediate notification of healthcare providers when vital signs deviate from established norms.
3. Implementing a robust architecture for data handling, real-time analysis, and automated alert communication.

## Features
Our system boasts a comprehensive set of features designed for seamless integration with cutting-edge technologies:

1. **Real-time data streams**: Continuously capture vital signs (blood pressure, heart rate, SpO2, respiration rate, temperature) from multiple patient devices every 30 seconds.
Intelligent alerts: Employ Spark Streaming to analyze vital signs against predefined thresholds and trigger instant alerts via AWS SNS when limits are exceeded.</br>
2. **Seamless communication**: Leverage AWS SNS to disseminate alerts and patient data to designated healthcare professionals through email or SMS.</br>
3. **Data enrichment**: Enrich the data landscape by sending vital signs and processed alerts to AWS S3 for future analysis and insights.</br>
4. **Visual analytics**: Integrate Metabase and AWS Redshift to create an intuitive dashboard for visualizing patient data trends and facilitating informed decision-making.</br>

## Project Flow
![image](https://github.com/SwaroopMeher/REAL-TIME-PATIENT-VITAL-MONITORING-SYSTEM/assets/115743490/430163d2-80d1-4153-9ec5-e56f5bfc09f9)

* **Data Ingestion**: Patient vital data streams from IoMT devices via Kafka Streams, updated every 30 seconds.</br>
* **Central Spark Streaming Application**:
  * Compares patient vitals against thresholds and generates alerts for exceeding values.
  * Exports vital data and processed alerts to AWS S3 for further analysis.</br>

* **Alerting**: AWS SNS receives alert data and sends notifications to healthcare professionals via email (or SMS).</br>
* **Data Analysis**: AWS Redshift stores and processes data from S3.</br>
* **Visualization**: Metabase utilizes Redshift tables to create a comprehensive dashboard for data visualization and trend analysis.</br>

## Results
* Successfully implemented a real-time patient monitoring system for vital signs and alert generation.</br>
* Established a robust and scalable architecture for handling data streams, real-time analysis, and automated alerts.</br>
* Developed a user-friendly dashboard for healthcare professionals to monitor patient trends, analyze data, and make informed decisions.</br>

![image](https://github.com/SwaroopMeher/REAL-TIME-PATIENT-VITAL-MONITORING-SYSTEM/assets/115743490/afbc396b-6bb4-421a-a0d3-3dce77d48fde)

![image](https://github.com/SwaroopMeher/REAL-TIME-PATIENT-VITAL-MONITORING-SYSTEM/assets/115743490/85e49f50-e01a-4e63-8d21-d8c62f6609d8)

* **Alert**:
![image](https://github.com/SwaroopMeher/REAL-TIME-PATIENT-VITAL-MONITORING-SYSTEM/assets/115743490/5bbde9ac-5ab2-46e7-80e1-1252174ab56b)


This project paves the way for a future of proactive healthcare, where real-time patient monitoring and intelligent alerts empower medical professionals to deliver exceptional care and improve patient outcomes.

## Next Steps
* Integrate additional vital signs and medical devices for comprehensive monitoring.
* Implement machine learning algorithms for predictive analysis and early detection of potential health issues.
* Develop mobile applications for healthcare professionals to access patient data and alerts on the go.
* Conduct clinical trials and obtain regulatory approvals for wider adoption in healthcare settings.

We believe this project holds immense potential to revolutionize healthcare by providing a foundation for continuous, proactive, and data-driven patient monitoring.

**Project report**: [Link](https://github.com/SwaroopMeher/REAL-TIME-PATIENT-VITAL-MONITORING-SYSTEM/blob/8dc13a86c03701e78d1ba3c5621822319bed3917/Shriansh_Swaroop_CS777_A1_Group_Report.pdf) </br>
**Project presentation**: [Link](https://github.com/SwaroopMeher/REAL-TIME-PATIENT-VITAL-MONITORING-SYSTEM/blob/8dc13a86c03701e78d1ba3c5621822319bed3917/Real-Time%20Patient%20Vital%20Monitoring%20System.pptx)

**Keywords**: Real-time patient monitoring, IoMT, Spark Streaming, AWS SNS, AWS S3, AWS Redshift, Metabase, healthcare analytics
