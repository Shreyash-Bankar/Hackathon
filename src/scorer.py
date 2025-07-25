import joblib, json
from fastapi import FastAPI
import uvicorn, pandas as pd

app = FastAPI()
model = joblib.load("models/gb.pkl")
enc   = joblib.load("models/enc.pkl")
scaler= joblib.load("models/scaler.pkl")

@app.post("/score")
async def score(event: dict):
    df = pd.DataFrame([event])
    cat = enc.transform(df[["proto","service","state","src_geo"]])
    num = scaler.transform(df[["dur","spkts","dpkts"]])
    X = pd.concat([pd.DataFrame(cat), pd.DataFrame(num)], axis=1)
    proba = model.predict_proba(X)[0,1]
    verdict = "HIGH_CONFIDENCE" if proba > 0.6 else "SUPPRESSED"
    return {"score": float(proba), "verdict": verdict}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
