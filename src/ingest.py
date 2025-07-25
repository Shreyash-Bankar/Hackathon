import json, random, time, glob, pandas as pd
from kafka import KafkaProducer

def stream_unsw(csv_folder="data/raw", topic="raw-logs"):
    prod = KafkaProducer(bootstrap_servers=["kafka:9092"],
                         value_serializer=lambda v: json.dumps(v).encode())
    cols = ["id","srcip","sport","dstip","dsport","proto","service",
            "state","dur","spkts","dpkts","ct_flw_http_mthd",
            "attack_cat","label"]
    for f in glob.glob(f"{csv_folder}/*.csv"):
        for row in pd.read_csv(f, usecols=cols, chunksize=1_000):
            for rec in row.to_dict(orient="records"):
                prod.send(topic, rec)
            time.sleep(0.01)   # throttle

if __name__ == "__main__":
    stream_unsw()
