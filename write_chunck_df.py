import concurrent.futures
import os
from utils import create_random_chunks, print_parameters, remove_all_files, time_it

@time_it
def main_parallel(output_dir: str, num_chunks: int, num_rows: int, num_columns: int) -> None:
    print_parameters(num_chunks, num_rows, num_columns, is_parallel=True)
    chunk_indices = range(1, num_chunks + 1)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(create_random_chunks, 
                     chunk_indices,
                     [num_rows]*num_chunks,
                     [num_columns]*num_chunks, 
                     [output_dir]*num_chunks)


@time_it
def main_serial(output_dir: str, num_chunks: int, num_rows: int, num_columns: int) -> None:
    print_parameters(num_chunks, num_rows, num_columns, is_parallel=False)
    chunk_indices = range(1, num_chunks + 1)
    for chunk_index in chunk_indices:
        create_random_chunks(chunk_index, num_rows, num_columns, output_dir)

if __name__ == "__main__":
    output_dir = "data_chunks"
    num_chunks = 100  # Number of chunks
    num_rows = 1000  # Number of rows in each chunk
    num_columns = 100  # Number of columns
    main_to_run = "all"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    print("Type of writing: ", main_to_run, "\n")
    
    if main_to_run == "parallel":
        main_parallel(
            output_dir=output_dir, 
            num_chunks=num_chunks,
            num_rows=num_rows,
            num_columns=num_columns,
        )
        remove_all_files(output_dir)

    elif main_to_run == "serial":
        main_serial(
            output_dir=output_dir, 
            num_chunks=num_chunks,
            num_rows=num_rows,
            num_columns=num_columns,
        )
        remove_all_files(output_dir)

    elif main_to_run == "all":
        main_serial(
            output_dir=output_dir, 
            num_chunks=num_chunks,
            num_rows=num_rows,
            num_columns=num_columns,
        )
        remove_all_files(output_dir)
        main_parallel(
            output_dir=output_dir, 
            num_chunks=num_chunks,
            num_rows=num_rows,
            num_columns=num_columns,
        )
        remove_all_files(output_dir)
    else:
        raise ValueError

