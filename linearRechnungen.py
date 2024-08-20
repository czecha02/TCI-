import pandas as pd
import numpy as np
from sqlalchemy import text
from connection import engine
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

# Funktion zur Berechnung der linearen Regression und Vorhersage
def calculate_linear_regression(df):
    # Um sicherzustellen, dass die Daten sortiert sind
    df = df.sort_values(by='Key Date').reset_index(drop=True)
    
    # Erstellen des Zeit/Rechenabschnitts
    df['Zeit/Rechen abschnitte'] = np.arange(len(df))
    
    # Entfernen von NaN-Werten aus den Spalten 'Value' und 'Zeit/Rechen abschnitte'
    df_clean = df.dropna(subset=['Value'])
    
    if len(df_clean) < 2:
        # Wenn nach dem Entfernen der NaN-Werte nicht genügend Daten für eine Regression übrig sind, überspringen
        df['LinearPredictedValue'] = np.nan
        df['LinearBestimmtheitsgrad'] = np.nan
        return df
    
    # Lineares Regressionsmodell erstellen
    X = df_clean['Zeit/Rechen abschnitte'].values.reshape(-1, 1)
    y = df_clean['Value'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    # Vorhersagen für vorhandene Zeitpunkte
    df['LinearPredictedValue'] = model.predict(df['Zeit/Rechen abschnitte'].values.reshape(-1, 1))
    
    # R²-Wert berechnen
    r2 = r2_score(y, model.predict(X)) * 100
    df['LinearBestimmtheitsgrad'] = r2
    
    # Vorhersagen für zukünftige Zeitpunkte
    future_dates = pd.date_range(df['Key Date'].max(), periods=4, freq='Q')[1:]
    future_df = pd.DataFrame({
        'Key Date': future_dates,
        'Business ID': df['Business ID'].iloc[0],
        'Value': np.nan,
        'Zeit/Rechen abschnitte': np.arange(len(df), len(df) + 3),
        'LinearPredictedValue': model.predict(np.arange(len(df), len(df) + 3).reshape(-1, 1)),
        'LinearBestimmtheitsgrad': r2
    })
    
    # Zusammenführen der vorhandenen Daten mit den zukünftigen Zeitpunkten
    return pd.concat([df, future_df], ignore_index=True)

# SQL-Abfrage, um die Daten für jede Business ID zu laden
query = "SELECT * FROM ldp"

# Daten aus der Datenbank laden
df = pd.read_sql(query, engine)

# Ergebnisse initialisieren
results = []

# Anwendung der Berechnung für jede Business ID
for business_id, group in df.groupby('Business ID'):
    results.append(calculate_linear_regression(group))

# Zusammenführen aller Ergebnisse
final_df = pd.concat(results, ignore_index=True)

# Daten zurück in die Datenbank schreiben
final_df.to_sql('ldp', con=engine, if_exists='replace', index=False)

print("Die Berechnung der linearen Regression und die Speicherung der Ergebnisse in der Datenbank war erfolgreich.")
