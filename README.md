# How to Read This Repository

This mini-project demonstrates how to use a multiprocessing library to compare the performance of I/O processes when writing chunks of data. The aim is to evaluate the differences between concurrent writing (using the `concurrent` library) and serial writing.

## Description

This repository contains two Python scripts:

- **`write_chunk_df.py`**: The core of the project, featuring two `main` functions that execute parallel and serial writing.
- **`utils.py`**: A utility package that provides functions to write chunks, measure execution time, and remove files.

## How to Use the Package

1. Open `write_chunk_df.py` and configure the parameters you wish to use:
   - Number of chunks to write
   - Number of columns in each chunk
   - Number of rows in each chunk
   - Type of simulation to run:
     - `"parallel"`: Run only the parallel write
     - `"serial"`: Run only the serial write
     - `"all"`: Run both processes

2. Execute the script using the following command:
   ```bash
   python3 write_chunk_df.py
   ```


## Required Libraries
To run this project, ensure you have the following libraries installed:

- Pandas
- NumPy