# extract/execute.py
import os, sys
from zipfile import ZipFile

def main():
    if len(sys.argv) != 2:
        print("Usage: python extract/execute.py <output_directory>")
        sys.exit(1)
    
    output_path = sys.argv[1]
    input_path = "data/fifa1.zip"

    print(f"Extracting {input_path} to {output_path}...")
    os.makedirs(output_path, exist_ok=True)
    
    with ZipFile(input_path, "r") as zip_file:
        zip_file.extractall(output_path)

    print("Extraction successful.")

if __name__ == "__main__":
    main()