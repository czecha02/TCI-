import pandas as pd
from sqlalchemy import create_engine
from connection import engine

# SQL-Abfrage, um alle Daten aus der Tabelle zu lesen
sql = "SELECT `Key Date`, `Business ID`, `Value`, `Within_2Std_Range` FROM ldp;"

# Daten aus der Datenbank abrufen und in ein DataFrame laden
df = pd.read_sql_query(sql, engine)

# Funktion zur Berechnung der Within_2Std_Range
def calculate_within_2std_range(row):
    mean = df[df['Business ID'] == row['Business ID']]['Value'].mean()
    std_dev = df[df['Business ID'] == row['Business ID']]['Value'].std()
    
    lower_bound = mean - 2 * std_dev
    upper_bound = mean + 2 * std_dev
    
    if lower_bound <= row['Value'] <= upper_bound:
        return True
    else:
        return False

# Neue Spalte 'Within_2Std_Range' basierend auf der Funktion aktualisieren
df['Within_2Std_Range'] = df.apply(calculate_within_2std_range, axis=1)

# Die aktualisierten Daten in die Datenbank schreiben, dabei die Spalte Within_2Std_Range aktualisieren
df.to_sql('ldp', con=engine, if_exists='replace', index=False, chunksize=1000)

# Bestätigung ausgeben
print("Die Spalte Within_2Std_Range wurde erfolgreich aktualisiert.")
