import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from connection import engine  # Stelle sicher, dass die Verbindung korrekt ist

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

# Überprüfe, ob die Spalte 'Within_Range' existiert und füge sie hinzu, falls nicht
if not column_exists(engine, 'ldp', 'Within_Range'):
    with engine.connect() as connection:
        try:
            connection.execute(text("ALTER TABLE ldp ADD COLUMN `Within_Range` BOOLEAN"))
        except ProgrammingError as e:
            print(f"Fehler beim Hinzufügen der Spalte: {e}")

# SQL-Abfrage, um alle Daten aus der Tabelle zu lesen
sql = "SELECT `Key Date`, `Business ID`, `Value` FROM ldp;"
df = pd.read_sql_query(sql, engine)
df['Key Date'] = pd.to_datetime(df['Key Date'])

# Sortiere nach 'Business ID' und 'Key Date'
df = df.sort_values(by=['Business ID', 'Key Date'])

# Fülle NaN-Werte in 'Value' mit 0
df['Value'] = df['Value'].fillna(0)

# Berechne den 'Change' Wert für jede 'Business ID'
df['Change'] = df.groupby('Business ID')['Value'].diff().fillna(0)

# Berechne Mittelwert und Standardabweichung für jede 'Business ID'
df['Mean'] = df.groupby('Business ID')['Value'].transform('mean')
df['Std'] = df.groupby('Business ID')['Value'].transform('std')

# Berechne die 'Within_Range' Spalte
df['Within_Range'] = (df['Value'] >= df['Mean'] - 2 * df['Std']) & (df['Value'] <= df['Mean'] + 2 * df['Std'])

# Ausgabe des DataFrames zur Überprüfung
print(df)
print(df[['Key Date', 'Business ID', 'Value', 'Change', 'Within_Range']])

# Aktualisiere die 'Within_Range' Spalte in der Datenbank
with engine.connect() as connection:
    transaction = connection.begin()
    try:
        for index, row in df.iterrows():
            sql_update = text("""
                UPDATE ldp
                SET `Within_Range` = :within_range
                WHERE `Key Date` = :key_date AND `Business ID` = :business_id;
            """)
            print(f"Updating row {index}: {row['Key Date']}, {row['Business ID']}, {row['Within_Range']}")
            connection.execute(sql_update, {
                'within_range': row['Within_Range'],
                'key_date': row['Key Date'],
                'business_id': row['Business ID']
            })
        transaction.commit()
    except Exception as e:
        transaction.rollback()
        print(f"Fehler beim Aktualisieren der Daten: {e}")

print("Die Spalte Within_Range wurde erfolgreich aktualisiert.")
