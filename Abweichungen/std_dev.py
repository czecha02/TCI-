import pandas as pd
import numpy as np  # Import für numpy hinzugefügt
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from connection import engine  # Verbindung zum Datenbank-Engine

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

# Überprüfe, ob die Spalte 'Standard Deviation' existiert und füge sie hinzu, falls nicht
if not column_exists(engine, 'ldp', 'Standard Deviation'):
    with engine.connect() as connection:
        try:
            connection.execute(text("ALTER TABLE ldp ADD COLUMN `Standard Deviation` FLOAT"))
        except ProgrammingError as e:
            print(f"Fehler beim Hinzufügen der Spalte 'Standard Deviation': {e}")

# SQL-Abfrage zum Abrufen der Daten
sql = "SELECT `Key Date`, `Business ID`, `Value` FROM ldp;"

# Lesen der SQL-Abfrage in einen DataFrame
df = pd.read_sql_query(sql, engine)

# Berechnung der Standardabweichung für jede 'Business ID'
df_std = df.groupby("Business ID")['Value'].std().reset_index()
df_std.columns = ['Business ID', 'Standard Deviation']

# Ersetze NaN-Werte durch None
df_std['Standard Deviation'] = df_std['Standard Deviation'].replace({pd.NA: None, np.nan: None})

# Ausgabe des DataFrames zur Überprüfung
print(df_std)

# Aktualisiere die 'Standard Deviation' Spalte in der Datenbank
with engine.connect() as connection:
    transaction = connection.begin()
    try:
        for index, row in df_std.iterrows():
            std_dev = row['Standard Deviation']
            if pd.isna(std_dev):
                std_dev = None
            sql_update = text("""
                UPDATE ldp
                SET `Standard Deviation` = :std_dev
                WHERE `Business ID` = :business_id;
            """)
            connection.execute(sql_update, {
                'std_dev': std_dev,
                'business_id': row['Business ID']
            })
        transaction.commit()
        print("Die Spalte 'Standard Deviation' wurde erfolgreich aktualisiert.")
    except Exception as e:
        transaction.rollback()
        print(f"Fehler beim Aktualisieren der Daten: {e}")
