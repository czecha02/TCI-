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

# Überprüfe, ob die Spalte 'SteigungTrend' existiert und füge sie hinzu, falls nicht
if not column_exists(engine, 'ldp', 'SteigungTrend'):
    with engine.connect() as connection:
        try:
            connection.execute(text("ALTER TABLE ldp ADD COLUMN `SteigungTrend` FLOAT"))
        except ProgrammingError as e:
            print(f"Fehler beim Hinzufügen der Spalte 'SteigungTrend': {e}")

# SQL-Abfrage zum Abrufen der Daten
sql = "SELECT `Key Date`, `Business ID`, `Value` FROM ldp;"

# Lesen der SQL-Abfrage in einen DataFrame
df = pd.read_sql_query(sql, engine)

# Sortieren des DataFrames nach `Key Date`
df_sorted = df.sort_values(by=['Key Date'])

# Konvertieren von `Key Date` in ein numerisches Format (z.B. Anzahl der Tage seit einem bestimmten Datum)
df_sorted['Key Date'] = pd.to_datetime(df_sorted['Key Date'])
df_sorted['Days'] = (df_sorted['Key Date'] - pd.to_datetime('2022-01-01')).dt.days

# Berechnung der Änderung (`Change`) basierend auf aufeinanderfolgenden Werten
df_sorted['Change'] = df_sorted.groupby('Business ID')['Value'].diff()

# Entfernen von NaN-Werten in der Spalte `Change`
df_sorted = df_sorted.dropna(subset=['Change'])

# Gruppieren nach `Business ID`
grouped_df = df_sorted.groupby("Business ID")

# Funktion zur Berechnung der Steigungstrends basierend auf Änderungswerten
def calculate_slope_trend(group):
    X = group['Days'].values.reshape(-1, 1)  # Verwenden der `Days`-Spalte als unabhängige Variable
    y = group['Change'].values  # Verwenden der `Change`-Spalte als Zielvariable
    
    model = LinearRegression()
    model.fit(X, y)
    
    slope_trend = model.coef_[0]
    return slope_trend

# Berechnung der Steigungstrends für jede Gruppe
slope_trend_df = grouped_df.apply(lambda group: pd.Series({'SteigungTrend': calculate_slope_trend(group)})).reset_index()

# Ausgabe des DataFrames zur Überprüfung
print(slope_trend_df)

# Aktualisiere die 'SteigungTrend' Spalten in der Datenbank
with engine.connect() as connection:
    transaction = connection.begin()
    try:
        for index, row in slope_trend_df.iterrows():
            sql_update = text("""
                UPDATE ldp
                SET `SteigungTrend` = :slope_trend
                WHERE `Business ID` = :business_id;
            """)
            connection.execute(sql_update, {
                'slope_trend': row['SteigungTrend'],
                'business_id': row['Business ID']
            })
        transaction.commit()
        print("Die Spalte 'SteigungTrend' wurde erfolgreich aktualisiert.")
    except Exception as e:
        transaction.rollback()
        print(f"Fehler beim Aktualisieren der Daten: {e}")
