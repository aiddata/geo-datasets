import os
import hashlib
from boxsdk import OAuth2, Client

auth = OAuth2(
    client_id="YOUR_CLIENT_ID",
    client_secret="",
    access_token="YOUR_DEVELOPER_TOKEN",
)

def sha1(filename):
    h  = hashlib.sha1()
    b  = bytearray(128*1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


def download_items(box_folder, dst_folder, skip_existing=True):
    os.makedirs(dst_folder, exist_ok=True)
    for i in box_folder.get_items():
        dst_file = dst_folder + i.name
        if skip_existing and os.path.isfile(dst_file):
            if sha1(dst_file) == i.sha1:
                print(f"File already exists with correct hash, skipping: {dst_file}")
                continue
            else:
                print(f"File already exists with incorrect hash, downloading again: {dst_file}")
        else:
            print(f"Downloading: {dst_file}")
        with open(dst_folder + i.name, "wb") as dst:
            i.download_to(dst)


if __name__ == "__main__":
    # create Box client
    client = Client(auth)
    # find shared folder
    shared_folder = client.get_shared_item("https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25")
    # find Global folder
    for i in shared_folder.get_items():
        if i.name == "Global":
            global_item = i
    # find Annual and Monthly child folders
    for i in global_item.get_items():
        if i.name == "Annual":
            annual_item = i
        if i.name == "Monthly":
            monthly_item = i
    # download Annual files, if we found them
    if annual_item:
        download_items(annual_item, "input_data/Annual/")
    else:
        print("Couldn't find an Annual folder")
    # download Monthly files, if we found them
    if monthly_item:
        download_items(monthly_item, "input_data/Monthly/")
    else:
        print("Couldn't find a Monthly folder")
