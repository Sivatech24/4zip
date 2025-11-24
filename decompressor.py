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


def decompress_file(dict_bin, input_bin, output_bin):
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

    # Decode index bit patterns
    full_bits = ""

    with open(input_bin, "rb") as f_in:
        while True:
            idx = varint_decode(f_in)
            if idx is None:
                break
            full_bits += dictionary.get(idx, "")

    # Convert bits → bytes
    out_bytes = bytearray()
    for i in range(0, len(full_bits), 8):
        byte_bits = full_bits[i:i+8]
        if len(byte_bits) == 8:
            out_bytes.append(int(byte_bits, 2))

    with open(output_bin, "wb") as f_out:
        f_out.write(bytes(out_bytes))


if __name__ == "__main__":
    decompress_file("indexfile.bin", "output.bin", "restored.bin")
    print("Decompression done.")