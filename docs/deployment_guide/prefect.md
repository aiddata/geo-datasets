# Deploying to Prefect

1. Login to Prefect
2. Make sure you have your conda environment activated
   ```
   conda activate geodata311
   ```
3. `cd` to the root of the repository
4. Run `deploy.py` from the `scripts` directory, passing the name of the dataset directory (without "/datasets/") as an argument
   ```
   python scripts/deploy.py esa_landcover
   ```
