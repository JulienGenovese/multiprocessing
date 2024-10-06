import os
import time
import pandas as pd
import numpy as np

def time_it(func):
    def wrapper(*args, **kwargs):
        print("-----------------\n")
        start_time = time.time()  # Record the start time
        result = func(*args, **kwargs)  # Call the actual function
        end_time = time.time()  # Record the end time
        print(f"Execution time: {end_time - start_time:.4f} seconds")
        print("\n-----------------\n")
        return result
    return wrapper

def print_parameters(num_chunks, num_rows, num_columns, is_parallel):
    print("Parameters:")
    print(f"Number of Chunks: {num_chunks}")
    print(f"Number of Rows per Chunk: {num_rows}")
    print(f"Number of Columns per Chunk: {num_columns}")
    print(f"Is parallel: {is_parallel}")

def remove_all_files(folder_path):
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Error while deleting file {file_name}: {e}")
    else:
        print(f"The folder {folder_path} does not exist or is not a directory.")


def create_random_chunks(i_chunck, num_rows, num_columns, output_dir):
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        raise FileNotFoundError()

    # Generate a DataFrame with random values
    df = pd.DataFrame(np.random.rand(num_rows, num_columns), 
                          columns=[f'col_{j+1}' for j in range(num_columns)])
    # Create a file name for the chunk
    file_name = os.path.join(output_dir, f'chunk_{i_chunck}.csv')
    # Write the DataFrame to a CSV file
    df.to_csv(file_name, index=False)
