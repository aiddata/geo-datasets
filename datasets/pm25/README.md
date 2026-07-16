# Surface PM2.5

The [Surface PM2.5 dataset](https://sites.wustl.edu/acag/datasets/surface-pm2-5/) from the [Atmospheric Composition Analysis Group](https://sites.wustl.edu/acag/) at Washington University in St. Louis.

## Set up your Box application

A Box application is needed to automatically download this dataset from Box.

If you have an enterprise account (e.g. from your university) these steps may require authorization by your enterprise admin; in that case a separate personal Box account may be easiest. Other authorization methods are available — see the [boxsdk auth docs](https://github.com/box/box-python-sdk/blob/main/docs/usage/authentication.md).

1. Make a Box account, or sign into an existing one.
2. Open the Box Dev Console at https://developer.box.com/
3. Create a new app: "Custom App", "Server Authentication (with JWT)", name it whatever you want.
4. On the app's Configuration page, under "Application Scopes", enable "Write all files and folders stored in Box" (required to download files).
5. Under "Advanced Features", enable "Generate user access tokens".
6. Under "Add and Manage Public Keys", click "Generate a Public/Private Keypair" (requires 2-factor auth) and move the downloaded JSON to `box_login_config.json` in this directory.
7. On the app's Authorization page, submit it for authorization.
8. Authorize the application from your Box Admin page (following the emailed link is easiest).

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `years` is a comma-separated list of years to process
    - `raw_dir` / `output_dir` are the download and output directories
    - `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them

## Manual downloading

If you'd rather not set up the Box application, download the data manually:

1. Create an `input_data` subdirectory.
2. Open the [data on Box](https://wustl.app.box.com/v/ACAG-V5GL04-GWRPM25) and download it.
3. Extract the zip into `input_data` so it contains `Annual/` and `Monthly/` subdirectories of `.nc` files.
4. Disable the data download step in `main.py`.
