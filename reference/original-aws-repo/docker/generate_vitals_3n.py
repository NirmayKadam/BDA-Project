import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta

# Function to generate patient vitals
def generate_patient_vitals(id, date_time):
    t=3
    n=1
    patient_data_10 = []
    for id in range(1,500001):
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

        #date_time= datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        data = {
            'Patient ID': id,
            'Heart Rate': heart_rate,
            'Systolic BP': systolic_bp,
            'Diastolic BP': diastolic_bp,
            'Temperature': temperature,
            'Respiration Rate': respiration_rate,
            'SpO2': spo2,
            'Datetime': date_time
        }

        patient_data_10.append(data)
    return patient_data_10

# Function to generate rows for 3 nights
def generate_rows_for_N_nights(N):
    start_date = datetime.strptime('2023-11-23 18:00:00', '%Y-%m-%d %H:%M:%S')
    end_date = start_date + timedelta(days=N)  # 3 nights

    time_interval = timedelta(seconds=30)
    current_date_time = start_date

    rows = []

    while current_date_time < end_date:
        if 18 <= current_date_time.hour < 20: #or 0 <= current_date_time.hour < 6:
            date_time_str = current_date_time.strftime('%Y-%m-%d %H:%M:%S')
            patient_data_n = generate_patient_vitals(id=1, date_time=date_time_str)
            for patient_data in patient_data_n: 
                rows.append(patient_data)

        current_date_time += time_interval

    return rows

# Generate rows for 3 nights
generated_rows = generate_rows_for_N_nights(1)

# Create a DataFrame from the generated rows
df = pd.DataFrame(generated_rows)
print(df.shape)
# Save the DataFrame to a CSV file
df.to_csv('data/patient_vitals_1night_500Kpatients.csv', index=False)
