import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from connection import engine  

pd.set_option('display.max_rows', None)

# Funktion zur Berechnung der Volatilität
def calculate_volatility(df):
    df['Volatility'] = df.groupby('Business ID')['Value'].transform('std')
    return df

# Funktion zur Aktualisierung der Volatilität in der Datenbank
def update_volatility_to_database(df, engine):
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            for index, row in df.iterrows():
                sql_update = text("""
                    UPDATE ldp
                    SET `Volatility` = :volatility
                    WHERE `Key Date` = :key_date AND `Business ID` = :business_id;
                """)
                connection.execute(sql_update, {
                    'volatility': row['Volatility'],
                    'key_date': row['Key Date'],
                    'business_id': row['Business ID']
                })
            transaction.commit()
            print("Die Spalte Volatility wurde erfolgreich aktualisiert.")
        except Exception as e:
            transaction.rollback()
            print(f"Fehler beim Aktualisieren der Volatility: {e}")

def add_column_if_not_exists(engine):
    with engine.connect() as connection:
        # Prüfe, ob die Spalte Volatility bereits existiert
        result = connection.execute(text("SHOW COLUMNS FROM ldp LIKE 'Volatility';")).fetchone()
        if result is None:
            # Füge die Spalte hinzu, wenn sie nicht existiert
            try:
                connection.execute(text("ALTER TABLE ldp ADD COLUMN `Volatility` FLOAT;"))
                print("Die Spalte Volatility wurde erfolgreich zur Tabelle hinzugefügt.")
            except ProgrammingError as e:
                print(f"Fehler beim Hinzufügen der Spalte Volatility: {e}")
        else:
            print("Die Spalte Volatility existiert bereits.")

if __name__ == "__main__":
    # SQL-Abfrage, um alle Daten aus der Tabelle zu lesen
    sql = "SELECT `Key Date`, `Business ID`, `Value` FROM ldp;"
    df = pd.read_sql_query(sql, engine)
    
    # Konvertiere 'Key Date' in ein DateTime-Objekt
    df['Key Date'] = pd.to_datetime(df['Key Date'])
    
    # Sortiere nach 'Business ID' und 'Key Date'
    df = df.sort_values(by=['Business ID', 'Key Date'])
    
    # Fülle NaN-Werte in 'Value' mit 0
    df['Value'] = df['Value'].fillna(0)
    
    # Berechne die Volatilität für jede 'Business ID'
    df = calculate_volatility(df)
    
    # Ausgabe des DataFrames zur Überprüfung
    print(df)
    print(df[['Key Date', 'Business ID', 'Value', 'Volatility']])
    
    # Füge die Spalte Volatility zur Tabelle hinzu, wenn sie noch nicht existiert
    add_column_if_not_exists(engine)
    
    # Aktualisiere die 'Volatility' Spalte in der Datenbank
    update_volatility_to_database(df, engine)
