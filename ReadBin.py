def read_bin_file(file_path):
    try:
        with open(file_path, "rb") as f:
            data = f.read()   # Reads all bytes
        print("File loaded successfully!")
        return data
    except Exception as e:
        print("Error:", e)
        return None


# Example usage:
bin_data = read_bin_file("output.bin")

if bin_data is not None:
    print("Size:", len(bin_data), "bytes")
    # Print first 50 bytes
    print("First 50 bytes:", bin_data[:50])
