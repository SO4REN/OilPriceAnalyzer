import os
import tarfile
import tempfile
import requests


if __name__ == "__main__":
    link_path = os.path.join(os.path.dirname(__file__), "README")

    response = None
    with open(link_path, "r") as f:
        link = f.readline().strip()
        response = requests.get(link)
        if response.status_code == 200:
            print("SUCCESS")
            response = response.content
        else:
            raise Exception("ERROR")

    with tempfile.TemporaryDirectory() as tmpdirname:
        targz_file = os.path.join(tmpdirname, "raw.tar.gz")
        with open(targz_file, "wb") as f:
            f.write(response)

        with tarfile.open(targz_file) as tar:
            tar.extractall(tmpdirname)
            tar.close()
            print("EXTRACTED")
        print(os.listdir(os.path.join(tmpdirname, "ftproot", "osservaprezzi")))
