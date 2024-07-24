import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from connection import engine  # Stelle sicher, dass die Verbindung zum Datenbank-Engine vorhanden ist

pd.set_option('display.max_rows', 10)

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

# Überprüfe, ob die Spalte 'Change' existiert und füge sie hinzu, falls nicht
if not column_exists(engine, 'ldp', 'Change'):
    with engine.connect() as connection:
        try:
            connection.execute(text("ALTER TABLE ldp ADD COLUMN `Change` FLOAT"))
        except ProgrammingError as e:
            print(f"Fehler beim Hinzufügen der Spalte: {e}")

# SQL-Abfrage für die Daten
sql = "SELECT `Key Date`, `Business ID`, `Value` FROM ldp;"

# Daten aus der Datenbank abrufen
df = pd.read_sql_query(sql, engine)

# Umwandlung von 'Key Date' in datetime falls nötig
df['Key Date'] = pd.to_datetime(df['Key Date'])

# Sortiere nach 'Business ID' und 'Key Date'
df = df.sort_values(by=['Business ID', 'Key Date'])

# Fülle NaN-Werte in 'Value' mit 0
df['Value'] = df['Value'].fillna(0)

# Berechne den 'Change' Wert für jede 'Business ID'
df['Change'] = df.groupby('Business ID')['Value'].diff().fillna(0)

# Ausgabe des DataFrames, um sicherzustellen, dass alle Zeilen verarbeitet wurden
print(df)

# Aktualisiere die 'Change' Spalte in der Datenbank
with engine.connect() as connection:
    transaction = connection.begin()
    try:
        for index, row in df.iterrows():
            sql_update = text("""
                UPDATE ldp
                SET `Change` = :change
                WHERE `Key Date` = :key_date AND `Business ID` = :business_id;
            """)
            connection.execute(sql_update, {
                'change': row['Change'],
                'key_date': row['Key Date'],
                'business_id': row['Business ID']
            })
        transaction.commit()
        print("Die Spalte 'Change' wurde erfolgreich aktualisiert.")
    except Exception as e:
        transaction.rollback()
        print(f"Fehler beim Aktualisieren der Daten: {e}")
