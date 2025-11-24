# input file name: 14769031_3840_2160_24fps.mp4

def convert_to_bin(input_file, output_file):
    try:
        with open(input_file, "rb") as infile:
            data = infile.read()

        with open(output_file, "wb") as outfile:
            outfile.write(data)

        print(f"Success! '{input_file}' converted to '{output_file}'.")
    except Exception as e:
        print("Error:", e)


# Example usage:
input_path = "14769031_3840_2160_24fps.mp4"      # change this to your file
output_path = "output.bin"    # output bin file name

convert_to_bin(input_path, output_path)