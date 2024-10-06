import pandas as pd
import numpy as np
import time
import concurrent.futures

import os


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


if __name__ == "__main__":
    output_dir = "data_chunks"
    num_chunks = 1000  # Number of chunks
    num_rows = 1000  # Number of rows in each chunk
    num_columns = 100  # Number of columns
    
    remove_all_files(output_dir)
    start_time = time.time()
    chunk_indices = range(1, num_chunks + 1)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(create_random_chunks, 
                     chunk_indices,
                     [num_rows]*num_chunks,
                     [num_columns]*num_chunks, 
                     [output_dir]*num_chunks)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'Total chunks created and written in {elapsed_time:.4f} seconds')
