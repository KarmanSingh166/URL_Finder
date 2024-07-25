import pandas as pd
from googlesearch import search
import requests
from bs4 import BeautifulSoup
import multiprocessing
import time

start = time.time()

def get_official_website(nbfc_name):
    query = f"{nbfc_name} official site"
    for url in search(query, num_results=10):
        if is_valid_official_website(url, nbfc_name):
            print(f'for {nbfc_name} is valid url = True and url is {url}')
            return url
    return None

def is_valid_official_website(url, nbfc_name):
    try:
        response = requests.get(url)
        response.encoding = response.apparent_encoding
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.title.string.lower() if soup.title else ""
            meta_description = ""
            meta_tag = soup.find('meta', attrs={'name': 'description'})
            if meta_tag and 'content' in meta_tag.attrs:
                meta_description = meta_tag['content'].lower()

            if nbfc_name.lower() in title or nbfc_name.lower() in meta_description:
                return True
        return False
    except Exception as e:
        return False

def process_row(row):
    nbfc_name = row['NBFC Name']
    if not isinstance(nbfc_name, str):
        return row.name, None
    nbfc_name = nbfc_name.replace("*", "").replace(".", "").replace("Ltd", "").replace("Limited", "").replace("[", "(").replace("]", ")").replace("{", "(").replace("}", ")").replace("  ", " ").strip()
    x = nbfc_name.find("(")
    if x >= 0:
        tempname = ''
        inBracket = False
        for lettt in range(0, len(nbfc_name)):
            lett = nbfc_name[lettt]
            if lettt > 0:
                if lett == " " and nbfc_name[lettt - 1] == " ":
                    continue
            if lett == "(":
                inBracket = True
            if not inBracket:
                tempname += lett
            if lett == ")":
                inBracket = False
        nbfc_name = tempname.strip()
    nbfc_name = str(nbfc_name)
    print(f"Processing {nbfc_name}\n\n")
    official_website = get_official_website(nbfc_name)
    return row.name, official_website

def process_rows(rows):
    results = []
    for _, row in rows.iterrows():
        result = process_row(row)
        results.append(result)
    return results

input_file = 'NBFCsandARCs10012023 (5).XLSX'
output_file = 'output_file.xlsx'
df = pd.read_excel(input_file, skiprows=1)
df.columns = [
    'SR No.', 'NBFC Name', 'Regional Office', 'Whether have CoR for holding/Accepting Public Deposits',
    'Classification', 'Corporate Identification Number', 'Layer', 'Address', 'Email ID', 'NaN'
]
df = df.drop(columns=['NaN'])
print("Column names:", df.columns.tolist())
df['Official Website'] = None

if __name__ == "__main__":
    count = 0
    try:
        num_chunks = multiprocessing.cpu_count() - 1
        chunks = [df.iloc[i::num_chunks] for i in range(num_chunks)]

        with multiprocessing.Pool(processes=num_chunks) as pool:
            results = pool.map(process_rows, chunks)

        results = [item for sublist in results for item in sublist]

        for index, website in results:
            if website is not None:
                df.at[index, 'Official Website'] = website
                count += 1

    except Exception as e:
        print(f"An error occurred: {e}")
        df.to_excel(output_file, index=False)
    else:
        df.to_excel(output_file, index=False)
        print("Completed! The output is saved in", output_file)

    end = time.time()
    print(f"Time taken = {end - start} seconds for {count} items")
