from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Chess Game Analysis") \
    .getOrCreate()

import re
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# Define a schema for the DataFrame
schema = StructType([
    StructField("black_rating", IntegerType(), True),
    StructField("white_rating", IntegerType(), True),
    StructField("time_control", StringType(), True),
    StructField("result", IntegerType(), True)
])

# Define the UDF for parsing PGN headers
def parse_pgn_headers(pgn):
    headers = {}
    # Use re.findall to find all matching header lines
    matches = re.findall(r'\[(\w+) "(.*?)"\]', pgn)
    for match in matches:
        # Update headers dictionary with each match
        headers[match[0]] = match[1]
    result = headers.get("Result", "unknown")
    if result == "1-0":
        result_code = 1
    elif result == "0-1":
        result_code = -1
    elif result == "1/2-1/2":
        result_code = 0
    else:
        result_code = -999  # unknown result code
    try:
        black_elo = int(headers.get("BlackElo", 0))
    except ValueError:
        black_elo = -999
    try:
        white_elo = int(headers.get("WhiteElo", 0))
    except ValueError:
        white_elo = -999
    time_control = headers.get("TimeControl", "unknown")
    return (black_elo, white_elo, time_control, result_code)

# Register the UDF
parse_pgn_udf = udf(parse_pgn_headers, schema)

# Read the whole PGN file as a single text
pgn_file_path = "lichess_db_standard_rated_2013-01.pgn"
with open(pgn_file_path, "r") as file:
    pgn_text = file.read()

# Split the PGN text into individual games
games = re.split(r'\n\n', pgn_text)

# Convert each game to a DataFrame row using the UDF
rows = []
for game in games:
    parsed_data = parse_pgn_headers(game)
    # Check if the parsed data is valid
    if parsed_data != (0, 0, "unknown", -999):
        rows.append(parsed_data)

# Create DataFrame from the rows
chess_df = spark.createDataFrame(rows, schema)

chess_df.show(5)

# # Write the DataFrame to Parquet format
# output_path = "chess_games.parquet"
# chess_df.write.parquet(output_path)

# # Print the path where the Parquet files are saved
# print("Parquet files are saved at:", output_path)
