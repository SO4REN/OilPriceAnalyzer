import os
import shutil
import tarfile
import tempfile
import requests

from splitImpianti import toMultipleFiles


def download(link):
    print("DOWNLOADING")
    response = requests.get(link)
    if response.status_code == 200:
        print("SUCCESS")
        return response.content
    else:
        raise Exception("ERROR")


if __name__ == "__main__":
    link_path = os.path.join(os.path.dirname(__file__), "README")

    response = None
    with open(link_path, "r") as f:
        link = f.readline().strip()
        response = download(link)

    with tempfile.TemporaryDirectory() as tmpdirname:
        targz_file = os.path.join(tmpdirname, "raw.tar.gz")
        with open(targz_file, "wb") as f:
            f.write(response)

        with tarfile.open(targz_file) as tar:
            tar.extractall(tmpdirname)
            tar.close()
            print("EXTRACTED")

        csv_path = os.path.join(tmpdirname, "ftproot", "osservaprezzi", "copied")
        anagrafica_path = os.path.join(os.path.dirname(__file__), "anagrafica_impianti_CT.parquet")
        output_path = os.path.join(os.path.dirname(__file__), "prezzi")

        if os.path.exists(output_path):
            shutil.rmtree(output_path)
            os.makedirs(output_path)
        toMultipleFiles(csv_path, output_path, anagrafica_path)
