import clickhouse_driver, pandas as pd, joblib, numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix

def load():
    cl = clickhouse_driver.Client("clickhouse")
    q = "SELECT * FROM logs"
    return pd.DataFrame(cl.execute(q, with_column_types=True)[0])

def train():
    df = load()
    y = df["label"]                 # 1 = attack; 0 = benign
    X = df.drop(columns=["id","label","attack_cat"])
    model = GradientBoostingClassifier(n_estimators=200, learning_rate=0.05,
                                       max_depth=3, subsample=0.8)
    model.fit(X, y)
    print(classification_report(y, model.predict(X)))
    joblib.dump(model, "models/gb.pkl")

if __name__ == "__main__":
    train()
