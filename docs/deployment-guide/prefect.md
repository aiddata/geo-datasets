# Deploying to Prefect

1. Enter the root directory of geo-datasets and make sure you have the `uv` CLI installed. If you don't have it yet, install it with:
   ```
   pip install uv
   ```
2. Make sure you have your uv environment synced. If you haven't done this yet, run the following command:
   ```
   uv sync
   ```
3. Link to Prefect Cloud and log in or connect to your self-hosted Prefect server. Make sure you have the correct API URL (and token for cloud) set in your environment variables.
   ```
   export PREFECT_API_URL=https://prefect.tail89de66.ts.net/api
   ```
4. Run `deploy.py` from the `scripts` directory, passing the name of the dataset directory (without "/datasets/") as an argument
   ```
   uv run scripts/deploy.py esa_landcover
   ```
5. You can now run deployments from the Prefect UI or CLI. To run via the Prefect UI, go to the Deployments menu, select which parameters you'd like to use, then submit the run. To run a deployment from the CLI, use the following command:
   ```
   prefect deployment run 'esa-landcover/ESA Landcover'
   ```
6. Note: All dataset scripts should be able to run locally by using:
   ```
   cd datasets/<dataset_name>
   uv run main.py
   ```
