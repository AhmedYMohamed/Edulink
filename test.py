import pandas as pd
import json
from pathlib import Path

json_directory = "classroom_data"

json_files = sorted(Path(json_directory).glob("classroom_data_*.json"),reverse=True)

with open(json_files[0],"r",encoding="UTF-8") as f:
    data = json.load(f)
    
df = pd.json_normalize(data)


print(df)