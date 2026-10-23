# Run after a new day's data appears so Hive sees the new partition.
docker exec pv-hive-server beeline -u jdbc:hive2://localhost:10000 -e "MSCK REPAIR TABLE patient_vitals.vitals"
