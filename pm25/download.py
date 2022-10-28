import os
import hashlib
from boxsdk import JWTAuth, Client

# adapted from https://stackoverflow.com/a/44873382
def sha1(filename):
    h  = hashlib.sha1()
    b  = bytearray(128*1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


def download_items(box_folder, dst_folder, skip_existing=True, verify_existing=True):
    """
    Downloads the contents of a Box folder to a dst_folder

    skip_existing will skip file names that already exist in dst_folder
    verify_existing will verify the hashes of existing files in dst_folder, if skip_existing is True
    """

    os.makedirs(dst_folder, exist_ok=True)

    for i in box_folder.get_items():
        dst_file = os.path.join(dst_folder, i.name)

        if skip_existing and os.path.isfile(dst_file):
            if verify_existing:
                if sha1(dst_file) == i.sha1:
                    print(f"File already exists with correct hash, skipping: {dst_file}")
                    continue
                else:
                    print(f"File already exists with incorrect hash, downloading again: {dst_file}")
            else:
                print(f"File already exists, skipping: {dst_file}")
                continue
        else:
            print(f"Downloading: {dst_file}")

        with open(dst_file, "wb") as dst:
            i.download_to(dst)


def download_data(**kwargs):
    """
    Downloads data from the Box shared folder for this dataset.

    kwargs are passed to download_items
    """

    # load JWT authentication JSON (see README.md for how to set this up)
    auth = JWTAuth.from_settings_file("box_login_config.json")

    # create Box client
    client = Client(auth)

    # find shared folder
    shared_folder = client.get_shared_item("https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25")

    # find Global folder
    for i in shared_folder.get_items():
        if i.name == "Global":
            global_item = i

    # raise a KeyError if Global directory cannot be found
    if not global_item:
        raise KeyError("Could not find directory \"Global\" in shared Box folder")

    # find Annual and Monthly child folders
    for i in global_item.get_items():
        if i.name == "Annual":
            annual_item = i
        if i.name == "Monthly":
            monthly_item = i

    # raise a KeyError if Annual or Monthly directories cannot be found
    if not annual_item:
        raise KeyError("Could not find directory \"Global/Annual\" in shared Box folder")
    elif not monthly_item:
        raise KeyError("Could not find directory \"Global/Monthly\" in shared Box folder")

    # download Annual files
    download_items(annual_item, "input_data/Annual/", **kwargs)

    # download Monthly files
    download_items(monthly_item, "input_data/Monthly/", **kwargs)

if __name__ == "__main__":
    download_data()
