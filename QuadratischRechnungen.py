# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import text
from connection import engine
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score

# Funktion zur Berechnung der linearen und quadratischen Regression und Vorhersage
def calculate_regressions(df):
    # Um sicherzustellen, dass die Daten sortiert sind
    df = df.sort_values(by='Key Date').reset_index(drop=True)
    
    # Erstellen des Zeit/Rechenabschnitts
    df['Zeit/Rechen abschnitte'] = np.arange(len(df))
    
    # Entfernen von NaN-Werten aus den Spalten 'Value' und 'Zeit/Rechen abschnitte'
    df_clean = df.dropna(subset=['Value'])
    
    if len(df_clean) < 2:
        # Wenn nach dem Entfernen der NaN-Werte nicht genügend Daten für eine Regression übrig sind, überspringen
        df['LinearPredictedValue'] = np.nan
        df['QuadratischePredictedValue'] = np.nan
        df['LinearBestimmtheitsgrad'] = np.nan
        df['QuadratischBestimmtheitsgrad'] = np.nan
        return df
    
    # Lineare Regression
    X = df_clean['Zeit/Rechen abschnitte'].values.reshape(-1, 1)
    y = df_clean['Value'].values
    
    linear_model = LinearRegression()
    linear_model.fit(X, y)
    
    df['LinearPredictedValue'] = linear_model.predict(df['Zeit/Rechen abschnitte'].values.reshape(-1, 1))
    
    # Quadratische Regression
    poly = PolynomialFeatures(degree=2)
    X_poly = poly.fit_transform(X)
    quadratic_model = LinearRegression()
    quadratic_model.fit(X_poly, y)
    
    df['QuadratischePredictedValue'] = quadratic_model.predict(poly.fit_transform(df['Zeit/Rechen abschnitte'].values.reshape(-1, 1)))
    
    # Bestimmtheitsgrade berechnen
    linear_r2 = r2_score(y, linear_model.predict(X)) * 100
    quadratic_r2 = r2_score(y, quadratic_model.predict(X_poly)) * 100
    
    df['LinearBestimmtheitsgrad'] = linear_r2
    df['QuadratischBestimmtheitsgrad'] = quadratic_r2
    
    # Vorhersagen für zukünftige Zeitpunkte (3 zusätzliche Quartale)
    future_dates = pd.date_range(df['Key Date'].max(), periods=4, freq='Q')[1:]
    future_df = pd.DataFrame({
        'Key Date': future_dates,
        'Business ID': df['Business ID'].iloc[0],
        'Value': np.nan,
        'Zeit/Rechen abschnitte': np.arange(len(df), len(df) + 3),
        'LinearPredictedValue': linear_model.predict(np.arange(len(df), len(df) + 3).reshape(-1, 1)),
        'QuadratischePredictedValue': quadratic_model.predict(poly.fit_transform(np.arange(len(df), len(df) + 3).reshape(-1, 1))),
        'LinearBestimmtheitsgrad': linear_r2,
        'QuadratischBestimmtheitsgrad': quadratic_r2
    })
    
    # Zusammenführen der vorhandenen Daten mit den zukünftigen Zeitpunkten
    return pd.concat([df, future_df], ignore_index=True)

# SQL-Abfrage, um die Daten für jede Business ID zu laden
query = "SELECT * FROM backup"

# Daten aus der Datenbank laden
df = pd.read_sql(query, engine)

# Ergebnisse initialisieren
results = []

# Anwendung der Berechnung für jede Business ID
for business_id, group in df.groupby('Business ID'):
    results.append(calculate_regressions(group))

# Zusammenführen aller Ergebnisse
final_df = pd.concat(results, ignore_index=True)

# Daten zurück in die Datenbank schreiben
final_df.to_sql('backup', con=engine, if_exists='replace', index=False)

print("Die Berechnungen der linearen und quadratischen Regression und die Speicherung der Ergebnisse in der Datenbank waren erfolgreich.")
