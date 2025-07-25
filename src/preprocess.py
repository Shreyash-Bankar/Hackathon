import pandas as pd, os, json, geoip2.database
from clickhouse_driver import Client
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
import joblib

GEO = geoip2.database.Reader("/opt/GeoLite2-City.mmdb")

def enrich(rec):
    rec["src_geo"] = GEO.city(rec["srcip"]).country.iso_code or "NA"
    rec["is_internal"] = rec["srcip"].startswith(("10.","192.168.","172.16."))
    rec["hour"] = pd.to_datetime("now").hour
    return rec

def batch_write(batch):
    cl = Client(host="clickhouse")
    cl.execute(
        "INSERT INTO logs FORMAT JSONEachRow",
        (json.dumps(r) for r in batch)
    )

def run():
    # Consume from Kafka, enrich, encode
    from kafka import KafkaConsumer
    enc = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
    scaler = StandardScaler()
    cons = KafkaConsumer("raw-logs", bootstrap_servers=["kafka:9092"],
                         value_deserializer=lambda m: json.loads(m.decode()))
    buffer = []
    for msg in cons:
        rec = enrich(msg.value)
        buffer.append(rec)
        if len(buffer) == 500:
            df = pd.DataFrame(buffer)
            cat = enc.fit_transform(df[["proto","service","state","src_geo"]])
            num = scaler.fit_transform(df[["dur","spkts","dpkts"]])
            X = pd.concat([pd.DataFrame(cat), pd.DataFrame(num)], axis=1)
            df[X.columns] = X
            batch_write(df.to_dict("records"))
            buffer.clear()
            joblib.dump(enc, "models/enc.pkl"); joblib.dump(scaler, "models/scaler.pkl")

if __name__ == "__main__":
    run()
