import os
import chardet

def detect_encoding(file_path, sample_size=10000):
    """Detect the encoding of a file."""
    with open(file_path, 'rb') as f:
        raw_data = f.read(sample_size)
        result = chardet.detect(raw_data)
        return result['encoding']

def check_file_encodings(directory='.'):
    """Check the encoding of all files in the directory."""
    files = os.listdir(directory)
    encoding_results = []

    for filename in files:
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            try:
                encoding = detect_encoding(file_path)
                encoding_results.append({
                    'Filename': filename,
                    'Encoding': encoding
                })
            except Exception as e:
                encoding_results.append({
                    'Filename': filename,
                    'Encoding': f"Error: {str(e)}"
                })

    # Output the results to the console
    print("Encoding Results:")
    for result in encoding_results:
        print(f"{result['Filename']}: {result['Encoding']}")

# Specify the directory you want to check
directory_to_check = './official'  # Change this to your desired directory
check_file_encodings(directory_to_check)
