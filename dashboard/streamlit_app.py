import streamlit as st, pandas as pd
from clickhouse_driver import Client
import requests, json, time

def get_latest(n=500):
    cl = Client("clickhouse")
    rows = cl.execute(f"SELECT * FROM logs ORDER BY id DESC LIMIT {n}")
    return pd.DataFrame(rows, columns=[c[0] for c in cl.execute("DESCRIBE TABLE logs")])

st.title("🔍 False-Positive Reducer")
df = get_latest()
st.metric("Logs Ingested", len(df))
if st.button("Refresh"):
    st.experimental_rerun()

scored = []
prog = st.progress(0)
for i,r in df.iterrows():
    resp = requests.post("http://scorer:8000/score", json=r.to_dict()).json()
    scored.append({**r, **resp})
    prog.progress((i+1)/len(df))
scored = pd.DataFrame(scored)

tab1, tab2 = st.tabs(["High-Confidence", "Suppressed"])
with tab1:
    st.dataframe(scored[scored.verdict=="HIGH_CONFIDENCE"])
with tab2:
    st.dataframe(scored[scored.verdict=="SUPPRESSED"])

# Analyst feedback
idx = st.selectbox("Mark a row as False Positive:", scored.index)
if st.button("Submit"):
    r = scored.loc[idx].to_dict()
    r["analyst_label"] = 0
    Client("clickhouse").execute(
        "INSERT INTO feedback FORMAT JSONEachRow", (json.dumps(r),)
    )
    st.success("Recorded. Model will retrain nightly!")
