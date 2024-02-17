import lz4.frame
import os

def decompress_files(date, coin, hours=24):
    base_path = os.path.join('data', date, coin)

    for hour in range(hours):
        lz4_file_path = os.path.join(base_path, f"{hour}.lz4")
        decompressed_file_path = os.path.join(base_path, f"{hour}.dat")

        try: 
            if os.path.exists(lz4_file_path):
                with open(lz4_file_path, 'rb') as lz4_file, open(decompressed_file_path, 'wb') as decompressed_file:
                    decompressed_file.write(lz4.frame.decompress(lz4_file.read()))
                print(f"Decompressed {hour}.lz4 successfully.")
                os.remove(lz4_file_path)
            else:
                print(f"File {hour}.lz4 not found.")
        except Exception as e:
            print(f"An error occurred while decompressing {hour}.lz4: {e}")


def compress_files(date, coin, hours=24):
    base_path = os.path.join('data', date, coin)

    for hour in range(hours):
        decompressed_file_path = os.path.join(base_path, f"{hour}.dat")
        lz4_file_path = os.path.join(base_path, f"{hour}.lz4")

        try:
            if os.path.exists(decompressed_file_path):
                with open(decompressed_file_path, 'rb') as decompressed_file, open(lz4_file_path, 'wb') as lz4_file:
                    lz4_file.write(lz4.frame.compress(decompressed_file.read()))
                print(f"Compressed {hour}.dat successfully.")
                os.remove(decompressed_file_path)
            else:
                print(f"File {hour}.dat not found.")
        except Exception as e:
            print(f"An error occurred while compressing {hour}.dat: {e}")

# Example usage
# decompress_files('20230916', 'SOL')  # Decompress all hours for this date and coin
# decompress_lz4_files('20230916', 'SOL', [0, 1, 2])  # Decompress specific hours
