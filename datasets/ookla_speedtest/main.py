from pathlib import Path
from typing import List
import os
from data_manager import BaseDatasetConfiguration, Dataset, get_config
from src.download_dataset import download_files
from src.helpers import GEOPARQUET_DIR, RASTER_OUTPUT_DIR


class OoklaSpeedtestConfiguration(BaseDatasetConfiguration):
    raw_dir: str = GEOPARQUET_DIR
    output_dir: str = RASTER_OUTPUT_DIR
    years: List[int] = [2019, 2020, 2021, 2022, 2023, 2024]
    overwrite_download: bool
    overwrite_processing: bool


class OoklaSpeedtest(Dataset):
    name = "Ookla Speedtest"

    def __init__(self, config: OoklaSpeedtestConfiguration):
        self.raw_dir = Path(config.raw_dir)
        self.output_dir = Path(config.output_dir)
        self.years = config.years
        self.overwrite_download = config.overwrite_download
        self.overwrite_processing = config.overwrite_processing

    def download(self, year: int, output_file_path=None) -> List[Path]:
        """
        Downloading a single year's worth of data
        """
        logger = self.get_logger()
        logger.info(f"Downloading parquet files for year {year}")
        return download_files(year)

    def process(self, input_path: Path, output_path: Path):
        logger = self.get_logger()

        if self.overwrite_download and not self.overwrite_processing:
            logger.warning("Overwrite download set but not overwrite processing.")
        if output_path.exists() and not self.overwrite_processing:
            logger.info(f"Processed layer exists: {output_path}")
        else:
            logger.info(
                f"Processing file: {input_path}. Ouput will be saved to: {output_path}"
            )

        # importing from src files
        from src.helpers import GRID_SIZE, NUM_BANDS, BAND16_COLS, BAND32_COLS
        from src.transform_populate import read_parquet
        from src.generate_raster import (
            make_raster_profile,
            write_multiband_raster_chunks,
        )

        # going through pipeline processing steps
        gdf = read_parquet(str(input_path))
        profile = make_raster_profile(num_bands=NUM_BANDS, grid_size=GRID_SIZE)

        write_multiband_raster_chunks(
            gdf=gdf,
            band32_cols=BAND32_COLS,
            band16_cols=BAND16_COLS,
            profile=profile,
            output_path=output_path,
        )
        return

    def main(self):
        logger = self.get_logger()

        # os.makedirs(self.raw_dir / "compressed", exist_ok=True)
        # os.makedirs(self.raw_dir / "uncompressed", exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)  # maybe necessary?

        # Download data
        logger.info("Running data download")
        download = self.run_tasks(self.download, [[y] for y in self.years])
        self.log_run(download)

        os.makedirs(self.output_dir, exist_ok=True)

        # Process data
        logger.info("Running processing")
        all_downloads = [f for result in download.results() for f in result]
        # ensuring output_path name name matches with the input_path name
        process_inputs = [
            (input_path, self.output_dir / f"{input_path.stem}.tif")
            for input_path in all_downloads
        ]
        process = self.run_tasks(self.process, process_inputs)
        self.log_run(process)


# ---- BEGIN BOILERPLATE ----
try:
    from prefect import flow
except Exception:
    pass
else:

    @flow
    def ookla_speedtest(config: OoklaSpeedtestConfiguration):
        OoklaSpeedtest(config).run(config.run)


if __name__ == "__main__":
    config = get_config(OoklaSpeedtestConfiguration)
    OoklaSpeedtest(config).run(config.run)
