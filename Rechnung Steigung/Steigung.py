import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from connection import engine

pd.set_option('display.max_rows', None)

# Funktion, um zu prüfen, ob eine Spalte in der Tabelle existiert
def column_exists(engine, table_name, column_name):
    query = text("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table_name AND COLUMN_NAME = :column_name
    """)
    with engine.connect() as connection:
        result = connection.execute(query, {'table_name': table_name, 'column_name': column_name})
        return result.scalar() > 0

# Überprüfe, ob die Spalte 'Steigung' existiert und füge sie hinzu, falls nicht
if not column_exists(engine, 'ldp', 'Steigung'):
    with engine.connect() as connection:
        try:
            connection.execute(text("ALTER TABLE ldp ADD COLUMN `Steigung` FLOAT"))
        except ProgrammingError as e:
            print(f"Fehler beim Hinzufügen der Spalte 'Steigung': {e}")

# SQL-Abfrage zum Abrufen der Daten
sql = "SELECT `Key Date`, `Business ID`, `Value` FROM ldp;"

# Lesen der SQL-Abfrage in einen DataFrame
df = pd.read_sql_query(sql, engine)

# Sortieren des DataFrames nach `Key Date`
df_sorted = df.sort_values(by=['Key Date'])

# Konvertieren von `Key Date` in ein numerisches Format (z.B. Anzahl der Tage seit einem bestimmten Datum)
df_sorted['Key Date'] = pd.to_datetime(df_sorted['Key Date'])
df_sorted['Days'] = (df_sorted['Key Date'] - pd.to_datetime('2022-01-01')).dt.days

# Entfernen von NaN-Werten in der Spalte `Value`
df_sorted = df_sorted.dropna(subset=['Value'])

# Gruppieren nach `Business ID`
grouped_df = df_sorted.groupby("Business ID")

# Funktion zur Berechnung der durchschnittlichen Steigung
def calculate_avg_slope(group):
    X = group['Days'].values.reshape(-1, 1)  # Verwenden der `Days`-Spalte als unabhängige Variable
    y = group['Value'].values  # Verwenden der `Value`-Spalte als Zielvariable
    
    model = LinearRegression()
    model.fit(X, y)
    
    avg_slope = model.coef_[0]
    return avg_slope

# Berechnung der durchschnittlichen Steigung für jede Gruppe
avg_slope_df = grouped_df.apply(lambda group: pd.Series({'Steigung': calculate_avg_slope(group)})).reset_index()

# Ausgabe des DataFrames zur Überprüfung
print(avg_slope_df)

# Aktualisiere die 'Steigung' Spalten in der Datenbank
with engine.connect() as connection:
    transaction = connection.begin()
    try:
        for index, row in avg_slope_df.iterrows():
            sql_update = text("""
                UPDATE ldp
                SET `Steigung` = :steigung
                WHERE `Business ID` = :business_id;
            """)
            connection.execute(sql_update, {
                'steigung': row['Steigung'],
                'business_id': row['Business ID']
            })
        transaction.commit()
        print("Die Spalte 'Steigung' wurde erfolgreich aktualisiert.")
    except Exception as e:
        transaction.rollback()
        print(f"Fehler beim Aktualisieren der Daten: {e}")
