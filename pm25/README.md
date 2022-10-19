# Surface PM2.5

This is the [Surface PM2.5 dataset](https://sites.wustl.edu/acag/datasets/surface-pm2-5/) from the [Atmospheric Composition Analysis Group](https://sites.wustl.edu/acag/) at Washington University in St. Louis.

## Downloading

### Manual Way
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
### Automated Way

1. Make a Box account, or sign into an existing one
2. Navigate to the Box Dev Console from [here](https://developer.box.com/)
3. Create a new app, select "Custom App," choose "Server Authentication," name it whatever you want
4. Go to your new app's Configuration page, and click Generate Developer Token
5. Copy Developer Token and OAuth Client ID into top of download.py in this folder
6. Install boxsdk Python package:
   ```
   pip install boxsdk
   ```
7. Run download.py from this directory:
   ```
   python download.py
   ```
8. The hour time limit runs out on your Developer Token, so you'll need to generate another one and re-run the script. It will automatically check the hashes of every file, skipping the ones that are good.

## Processing

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
