# Surface PM2.5

This is the [Surface PM2.5 dataset](https://sites.wustl.edu/acag/datasets/surface-pm2-5/) from the [Atmospheric Composition Analysis Group](https://sites.wustl.edu/acag/) at Washington University in St. Louis.

## How to Use

### Set up your Box Application

A Box application is necessary to automatically download this dataset from Box.

If you have an enterprise account with Box, e.g. an account given to you by your university, these steps will require an authorization by your enterprise admin.
In this case, it might be easiest for you to create a separate, personal Box account to use with this script.
Other authorization methods are available to you, see [this boxsdk docs page](https://github.com/box/box-python-sdk/blob/main/docs/usage/authentication.md) for more information.

1. Make a Box account, or sign into an existing one
2. Navigate to the Box Dev Console from [here](https://developer.box.com/)
3. Create a new app, select "Custom App," choose "Server Authentication (with JWT)", name it whatever you want
4. Go to your new app's Configuration page, scroll down to "Application Scopes", and make sure "Write all files and folders stored in Box" is enabled. This setting is necessary to download files from Box.
5. Scroll down to "Advanced Features" and enable "Generate user access tokens"
6. Scroll down to "Add and Manage Public Keys" and click on "Generate a Public/Private Keypair". Move the downloaded JSON file to `box_login_config.json` in this directory.
    - Note: This required 2-factor authentication to be set up. Follow prompt to do so after clicking "Generate a Public/Private Keypair" if not already enabled. Once enabled, click "Generate a Public/Private Keypair" again.
7. Navigate to your app's Authorization page, and submit your app for authorization from your enterprise admin. If you are using a personal account, you will receive an email to authorize the application.
8. Authorize the application from your Box Admin page (it is easiest to follow the link in the email).

### Install conda environment

- If you are using Linux, it's probably easiest to load up the existing conda environment in this repository:
  ```
  conda env create -f env.yaml
  conda activate pm25
  ```
- Alternatively, run `create_env.sh` to build the dependencies for your platform:
  ```
  chmod +x create_env.sh
  ./create_env.sh
  ```

### Tweak settings in `main.py`

Near the top of `main.py`, there are a few options you should review before running this project.
Choose whether or not you'd like to use prefect, and set run_parallel to True if you are using the HPC.

### Run script

The results will be written to the `output_data` subdirectory, which will be automatically created if it didn't exist already.

```
python main.py
```

If you are using the W&M HPC, tweak `jobscript` and then run `qsub jobscript`

## Manual Downloading

If you aren't comfortable setting up the Box application as described above, it is possible to download the data manually.

1. Make an input_data subdirectory, if you haven't already:
   ```
   mkdir -p input_data
   ```
2. Navigate to the [data hosted on Box](https://wustl.app.box.com/v/ACAG-V5GL02-GWRPM25), and click the download button in the upper left corner
3. Extract the zip file into input_data so that it looks like this:
   ```
   ├── input_data
   │   ├── Annual
   │   │   ├── V5GL02.HybridPM25.Global.199801-199812.nc
   │   │   ├── V5GL02.HybridPM25.Global.199901-199912.nc
   │   │   └── ...
   │   └── Monthly
   │       ├── V5GL02.HybridPM25.Global.199801-199801.nc
   │       ├── V5GL02.HybridPM25.Global.199802-199802.nc
   │       └── ...
   └── ...
   ```
4. Disable the data download step in `main.py`
