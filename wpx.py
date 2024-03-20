import pandas as pd
import tkinter as tk
from tkinter import filedialog
import numpy as np
from concurrent.futures import ThreadPoolExecutor


root = tk.Tk()
root.withdraw()

file_path = filedialog.askopenfilename()

#print(file_path)

#ULS Data File col names
column_names = ['Record Type', 'Unique System Identifier', 'ULS File Number', 'EBF Number', 'Call Sign']

data = pd.read_table(file_path,sep='|',header=None )

column_series = data[4].astype(str)


# Define a list of valid amateur radio callsign prefixes for the United States
us_prefixes = []

# Single-letter prefixes: K, N, W
us_prefixes.extend([f"{prefix}{num}" for prefix in ['K', 'N', 'W'] for num in range(0, 10)])

# Two-letter prefixes: AA-AL, KA-KZ, NA-NZ, WA-WZ
# AA-AL prefix range
us_prefixes.extend([f"A{prefix2}{num}" for num in range(0, 10) for prefix2 in [chr(j) for j in range(65, 77)]])
# KA-KZ, NA-NZ, WA-WZ prefix range
us_prefixes.extend([f"{prefix}{prefix2}{num}" for num in range(0, 10) for prefix in ['K', 'N', 'W'] for prefix2 in [chr(j) for j in range(65, 91)]])

# Filter out prefixes starting with KP, NP, or WP and ending with 0, 6, 7, 8, or 9
filtered_prefixes = []
for prefix in us_prefixes:
    if not any(prefix.startswith(p) and prefix.endswith(n) for p in ['KP', 'NP', 'WP'] for n in ['0', '6', '7', '8', '9']):
        filtered_prefixes.append(prefix)

# Initialize dictionary to store counts
prefix_counts = {}

# Function to calculate count for a prefix
def calculate_count(prefix):
    return prefix, column_series.str.contains(prefix).sum()

# Use ThreadPoolExecutor to run the calculations in parallel
with ThreadPoolExecutor() as executor:
    results = executor.map(calculate_count, filtered_prefixes)

# Populate the prefix_counts dictionary with results
for prefix, count in results:
    prefix_counts[prefix] = count

# Create DataFrame from the dictionary
df = pd.DataFrame(list(prefix_counts.items()), columns=['Prefix', 'count'])

# Display the DataFrame
print(df)

save_path = filedialog.asksaveasfilename()
df.to_csv(save_path, sep='\t', index=False)
