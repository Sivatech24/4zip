def varint_decode(f):
    shift = 0
    result = 0
    while True:
        b = f.read(1)
        if not b:
            return None
        byte = b[0]
        result |= ((byte & 0x7F) << shift)
        if byte < 128:
            break
        shift += 7
    return result

def decompress_file(dict_bin, input_bin, output_txt):
    dictionary = {}

    # Load dictionary
    with open(dict_bin, "rb") as f:
        while True:
            idx = varint_decode(f)
            if idx is None:
                break

            length = varint_decode(f)
            pattern = f.read(length).decode("ascii")

            dictionary[idx] = pattern

    # Rebuild output bitstream
    with open(input_bin, "rb") as f_in, open(output_txt, "w") as f_out:
        while True:
            idx = varint_decode(f_in)
            if idx is None:
                break
            pattern = dictionary.get(idx, "")
            f_out.write(pattern)


if __name__ == "__main__":
    decompress_file("indexfile.bin", "output.bin", "restored.txt")
    print("Decompression completed!")
