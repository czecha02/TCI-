import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Datenbank-Verbindung einrichten
from connection import engine

# Daten aus der Datenbank lesen
query = "SELECT * FROM ldp"
df = pd.read_sql(query, engine)

# Sicherstellen, dass 'Key Date' als Datetime-Typ vorliegt
df['Key Date'] = pd.to_datetime(df['Key Date'])

# Gruppierung der Daten nach 'Business ID'
grouped_df = df.groupby('Business ID')

def calculate_linear_predicted_values(group):
    # Sicherstellen, dass es mehr als einen Datenpunkt gibt
    if group.shape[0] < 2:
        print(f"Business ID {group['Business ID'].iloc[0]}: Nicht genügend Daten für Regression")
        group['LinearPredictedValue'] = np.nan
        return group

    # Umwandlung der 'Key Date' in numerische Werte (Tage seit dem ersten Datum)
    group = group.sort_values('Key Date')
    group['Days'] = (group['Key Date'] - group['Key Date'].min()).dt.days

    # Entfernen von Zeilen mit NaN-Werten
    group = group.dropna(subset=['Value'])
    
    # Wenn nach dem Entfernen von NaN-Werten keine Daten vorhanden sind
    if group.empty:
        print(f"Business ID {group['Business ID'].iloc[0]}: Keine Daten für Regression nach Entfernen von NaN-Werten")
        group['LinearPredictedValue'] = np.nan
        return group

    # Vorbereiten der Daten für die Regression
    X = group[['Days']]
    y = group['Value']
    
    model = LinearRegression()
    
    try:
        model.fit(X, y)
        group['LinearPredictedValue'] = model.predict(X)
        
        # Berechnung zukünftiger Daten
        last_date = group['Key Date'].max()
        future_dates = [last_date + timedelta(days=91*i) for i in range(1, 4)]
        
        future_df = pd.DataFrame({
            'Key Date': future_dates,
            'Business ID': group['Business ID'].iloc[0],
            'Value': np.nan
        })
        
        future_df['Days'] = (future_df['Key Date'] - group['Key Date'].min()).dt.days
        future_df['LinearPredictedValue'] = model.predict(future_df[['Days']])
        
        # Zusammenführen der originalen und zukünftigen Daten
        result_df = pd.concat([group, future_df], ignore_index=True)
        
    except Exception as e:
        print(f"Business ID {group['Business ID'].iloc[0]}: Fehler bei der Regression - {e}")
        group['LinearPredictedValue'] = np.nan
        result_df = group

    return result_df

# Anwendung der Funktion auf jede Gruppe
df_with_predictions = grouped_df.apply(calculate_linear_predicted_values).reset_index(drop=True)

# Ergebnisse in die Datenbank zurückschreiben oder in eine neue Tabelle speichern
# Beispielsweise können wir es in einer neuen Tabelle speichern:
df_with_predictions.to_sql('ldp_predictions', engine, if_exists='replace', index=False)

print("Daten mit Vorhersagen wurden erfolgreich berechnet und in die Tabelle 'ldp_predictions' geschrieben.")
