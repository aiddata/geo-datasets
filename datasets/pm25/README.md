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
6. Under "Add and Manage Public Keys", click "Generate a Public/Private Keypair" (requires 2-factor auth) and save the downloaded JSON as `box_config.json` in this directory (gitignored).
7. On the app's Authorization page, submit it for authorization.
8. Authorize the application from your Box Admin page (following the emailed link is easiest).

For local runs, put the same JSON minified to a single line in a gitignored `.env` in this directory:
```
box_config=<the minified JSON>
```
`scripts/deploy.py` reads `box_config.json` directly and passes it (minified) as the `box_config` parameter for Prefect deployments, so no `.env` entry is needed for that path.

## Quick start

1. Review and edit the variables in `config.toml` as needed
    - `years` is a comma-separated list of years to process
    - `raw_dir` / `output_dir` are the download and output directories
    - `overwrite_download` / `overwrite_processing`, if true, overwrite existing files rather than skip them
    - `box_config` — the Box JWT app-auth JSON; leave the `<ADD-…>` placeholder in `config.toml` and set the real value in `.env` (see Set up your Box application)

## Manual downloading

If you'd rather not set up the Box application, download the data manually:

1. Create an `input_data` subdirectory.
2. Open the [data on Box](https://wustl.app.box.com/v/ACAG-V5GL04-GWRPM25) and download it.
3. Extract the zip into `input_data` so it contains `Annual/` and `Monthly/` subdirectories of `.nc` files.
4. Disable the data download step in `main.py`.
