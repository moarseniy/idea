Как запускать:

pip install pandas numpy pyarrow

python profile_csv.py C:\Users\1\CU\data_for_data_engineer\csv_pipeline\data\raw\part-00001.csv --sep ";"  

python csv_to_parquet_full.py C:\Users\1\CU\data_for_data_engineer\csv_pipeline\data\raw\part-00001.csv --sep ";"

Резльтат - файл json и паркет рядом с сырым csv