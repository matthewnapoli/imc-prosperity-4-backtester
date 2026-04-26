import pandas as pd
import os

# --- SETTINGS ---
INPUT_DIR = r"C:\Users\mnapoli\Downloads\ROUND_4"
OUTPUT_DIR = r"C:\Users\mnapoli\Desktop\imc-prosperity-4-backtester\prosperity4bt\resources\round4"

# Mapping of file types to their column name for the product/symbol
FILES_TO_PROCESS = [
    {"prefix": "prices", "col": "product"},
    {"prefix": "trades", "col": "symbol"}
]

def split_data_by_product():
    # Ensure output directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    for day in [1, 2, 3]:
        for config in FILES_TO_PROCESS:
            file_type = config["prefix"]
            prod_col = config["col"]
            
            # Construct input filename
            filename = f"{file_type}_round_4_day_{day}.csv"
            input_path = os.path.join(INPUT_DIR, filename)
            
            if not os.path.exists(input_path):
                print(f"Skipping: {filename} not found in {INPUT_DIR}")
                continue
            
            print(f"Processing {filename}...")
            
            # Load the raw CSV (using ; as seen in your files)
            df = pd.read_csv(input_path, sep=';')
            
            # Get list of unique products in this file
            products = df[prod_col].unique()
            
            for product in products:
                # Create product subfolder
                product_dir = os.path.join(OUTPUT_DIR, product)
                os.makedirs(product_dir, exist_ok=True)
                
                # Filter data and RESET INDEX
                # drop=True prevents the old index from being added as a new column
                filtered_df = df[df[prod_col] == product].reset_index(drop=True)
                
                # Construct output filename
                output_filename = f"{file_type}_round_4_day_{day}_{product}.parquet"
                output_path = os.path.join(product_dir, output_filename)
                
                # Save as Parquet
                try:
                    filtered_df.to_parquet(output_path, engine='pyarrow')
                except ImportError:
                    print("Error: 'pyarrow' is not installed. Saving as CSV instead.")
                    filtered_df.to_csv(output_path.replace(".parquet", ".csv"), index=False, sep=';')

    print("\nDone! All files have been split, indexed, and saved.")

if __name__ == "__main__":
    split_data_by_product()