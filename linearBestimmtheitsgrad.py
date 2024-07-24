import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.metrics import r2_score
from connection import engine

# Load data from the table
query = "SELECT * FROM ldp_predictions"
df = pd.read_sql(query, engine)

# Convert `Key Date` to datetime
df['Key Date'] = pd.to_datetime(df['Key Date'])

# Group by 'Business ID' and calculate R² for each group
def calculate_r2(group):
    # Filter out future dates (we only need the existing data points)
    group_existing = group.dropna(subset=['Value'])  # Use existing values for R² calculation
    
    if len(group_existing) < 2:
        # Not enough data to compute R²
        group['BG_linear'] = np.nan
        return group
    
    y_true = group_existing['Value']
    y_pred = group_existing['LinearPredictedValue']
    
    # Calculate R²
    r2 = r2_score(y_true, y_pred)
    
    # Add R² to the existing data
    group['BG_linear'] = np.nan  # Initialize with NaN for all rows
    group.loc[group_existing.index, 'BG_linear'] = r2 * 100  # Store R² as a percentage
    
    return group

# Apply function to each group
df_with_r2 = df.groupby('Business ID').apply(calculate_r2).reset_index(drop=True)

# Update only the 'BG_linear' column in the existing table
with engine.connect() as conn:
    # Ensure the 'BG_linear' column exists in the DataFrame
    if 'BG_linear' not in df_with_r2.columns:
        df_with_r2['BG_linear'] = np.nan
    
    # Create or replace only the necessary columns in the table
    df_with_r2[['Key Date', 'Business ID', 'BG_linear']].to_sql('ldp_predictions', conn, if_exists='replace', index=False)

print("Die Tabelle 'ldp_predictions' wurde erfolgreich mit der Spalte 'BG_linear' aktualisiert.")
