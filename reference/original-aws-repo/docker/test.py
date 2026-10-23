
import numpy as np
import scipy.stats as stats
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
# from pyspark.sql.functions import to_json, struct
import json
import random
import time
from datetime import datetime, timedelta
from pytz import timezone
tz = timezone('EST')
import time


def generate_patient_vitals(id):
    t=2.43
    n=1
    # Simulate random values for heart rate (bpm)
    heart_rate = int(np.round(stats.truncnorm.rvs(-t, t, 72, 12, n), 2)[0])
    
    # Simulate random values for blood pressure (mmHg)
    systolic_bp = int(np.round(stats.truncnorm.rvs(-t, t, 115, 12, n), 2)[0])
    diastolic_bp = int(np.round(stats.truncnorm.rvs(-t, t, 75, 10, n), 2)[0])

    # Simulate random values for temperature (°C)
    temperature = np.round(stats.truncnorm.rvs(-t, t, 97, 0.5, n), 2)[0]

    # Simulate random values for respiration rate (breaths per minute)
    respiration_rate = int(np.round(stats.truncnorm.rvs(-t, t, 17, 3, n), 2)[0])

    # Simulate random values for SpO2 (%)
    spo2 = np.round(stats.truncnorm.rvs(-t, t, 98, 1.5, n), 2)
    spo2[spo2 > 100] = 100
    spo2=spo2[0]

    date_time= datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    return {
        'Patient ID': id,
        'Heart Rate': heart_rate,
        'Systolic BP': systolic_bp,
        'Diastolic BP': diastolic_bp,
        'Temperature': temperature,
        'Respiration Rate': respiration_rate,
        'SpO2': spo2,
        'Datetime': date_time
    }

def send_vitals():
    start = time.process_time()
    for id in range(1,50001):  # Run for 10 seconds
        generate_patient_vitals(id)
    print(f"Time take for one loop of {id-1} patients: ",time.process_time() - start)
    time.sleep(2)

if __name__ == "__main__":
    send_vitals()