import pandas as pd
import os

def convert_csv_to_toml(source_file_path: str, destination_file_path: str, version: str):
    redirects_arr = pd.read_csv(source_file_path)
    redirects = set([*map(tuple,redirects_arr.values)])

    output_rules = []
    for origin, destination in redirects:
        if not destination.startswith("/") and not destination.startswith("https"):
            destination = "/" + destination
        if not destination.endswith("/"):
            destination =  destination + "/"
        if not origin.startswith("/"):
            origin = "/" + origin
        if not origin.endswith("/"):
            origin =  origin + "/"
        output_rules.append(
            '\n[[redirects]] \rfrom = "'
            + origin
            + '"\rto = "'
            + destination
            + '"\r\r'
            )
    output_rules.sort()

    DESTINATION_FILE = f"{destination_file_path}"
    with open(DESTINATION_FILE, "w") as f:
        output_rules.insert(0, f'#{len(redirects)} redirects found specific to version {version}')
        f.write("".join(output_rules))

def main():
    file_name = 'netlify-bi-connector-current-redirects'

    source_file_path = f'../netlify-redirects/{file_name}.csv'
    if not os.path.isfile(source_file_path):
        print(f"Source file does not exist at path {source_file_path}")
        return 
    destination_file_path = f'../netlify-redirects/{file_name}.toml'
    convert_csv_to_toml(source_file_path, destination_file_path, file_name)

if __name__ == "__main__":
    main()