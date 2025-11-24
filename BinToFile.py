def bin_to_file(input_bin, output_file):
    try:
        # Read the binary data from the .bin file
        with open(input_bin, "rb") as infile:
            data = infile.read()

        # Write the data back to a normal file
        with open(output_file, "wb") as outfile:
            outfile.write(data)

        print(f"Success! '{input_bin}' restored to '{output_file}'.")
    except Exception as e:
        print("Error:", e)


# Example usage:
input_bin_path = "decompress/input.bin"   # your .bin file
output_file_path = "restored.mp4"  # output restored file

bin_to_file(input_bin_path, output_file_path)