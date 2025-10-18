import requests 
import gzip 
import io




URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-05/wet.paths.gz"


def get_and_decompress_stream_file(url):
    """Recevie a url to download a gz file
       Decompress and open file in memory.
       Get the urls inside of it. Download and stream the new file, open and print each line 
    """
    #get the file
    response = requests.get(URL,stream=True)
    response.raise_for_status()

    #wrap the binary content into a BytesIO buffer (file-like object in memory)
    compressed_stream = io.BytesIO(response.content)

    #decompress the file-like object stream
    with gzip.open(compressed_stream, 'rt',encoding='utf-8') as f:
        urls=[]
        for i,line in enumerate(f):
            url = 'https://data.commoncrawl.org/'+line.strip()
            print(url)
            urls.append(url)
            if i == 0:
                break
        for url in urls:
            #download the file
            with requests.get(url,stream=True) as r:
                    r.raise_for_status()
                    #decompress and print the lines inside the file stream
                    with gzip.open(r.raw, 'rt',encoding='utf-8') as f:
                        for i,line in enumerate(f):
                            print(line.strip())
                            if i==5:
                                break

def main():

    get_and_decompress_stream_file(URL)    

    


if __name__ == "__main__":
    main()
