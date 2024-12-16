##########################################

# This python script will be used for exploratory data analysis of "data/supporting_facilities.csv"

##########################################

import pandas as pd
import numpy as np
from pathlib import Path
import os
import seaborn as sns
import matplotlib.pyplot as plt


try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd()

# Go one level up from BASE_DIR using parent
DATA_DIR = BASE_DIR.parent / "data"

df = pd.read_csv(os.path.join(DATA_DIR, "supporting-facilities_clean.csv"))


# Test: histogram
plt.figure(figsize=(10, 6))  # Set figure size
plt.hist(df['nr_operating_theatres'], bins=30, edgecolor='black')
plt.xlabel('Number of Operating Theatres')
plt.ylabel('Frequency')
plt.title('Distribution of Operating Theatres')
plt.grid(True, alpha=0.3)
plt.show()




