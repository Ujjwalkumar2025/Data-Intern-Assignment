import pandas as pd
from pathlib import Path

def consolidate_data():
    input_dir = Path.home() / "Desktop" / "SoilHealthData"
    output_file = input_dir / "consolidated_data.csv"
    
    all_data = []
    
    for csv_file in input_dir.glob("**/*_MACRO.csv"):
        df = pd.read_csv(csv_file)
        df["Source"] = str(csv_file)
        all_data.append(df)
    
    if all_data:
        pd.concat(all_data).to_csv(output_file, index=False)
        print(f"Consolidated data saved to: {output_file}")
    else:
        print("No data files found")

if __name__ == "__main__":
    consolidate_data()
