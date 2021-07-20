
import os
import glob
import pandas as pd

from utility import run_tasks, get_current_timestamp, file_exists, convert_daily, concat_month, concat_year, agg_to_grid_month, agg_to_grid_year, interpolate_month, interpolate_year


data_url = "https://oco2.gesdisc.eosdis.nasa.gov/data/OCO2_DATA/OCO2_L2_Lite_FP.10r/"

timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')


# -------------------------------------

raw_dir = "/sciclone/aiddata10/REU/geo/raw/gesdisc/OCO2_L2_Lite_FP_V10r"

data_dir = "/sciclone/aiddata10/REU/geo/data/rasters/gesdisc/OCO2_L2_Lite_FP_V10r/xco2"

year_list = range(2015, 2021)

mode = "parallel"
# model = "serial"

max_workers = 10

overwrite = False

# -------------------------------------


# resolution of grid points and interpolated raster output
rnd_interval = 0.1
decimal_places = len(str(rnd_interval).split(".")[1])

# methods for aggregating raw data to regular grid
agg_ops = {
    'xco2': "mean",
    'xco2_quality_flag': "count",
    'longitude': "first",
    'latitude': 'first'
}

# interpolation method
interp_method = "linear"


run_a = True
run_b = True
run_c = True
run_d = True
run_e = True
run_f = True
run_g = True

# -------------------------------------




day_dir = os.path.join(data_dir, "day")
month_dir = os.path.join(data_dir, "month")
month_grid_dir = os.path.join(data_dir, "month_grid")
month_interp_dir = os.path.join(data_dir, "month_interp")
year_dir = os.path.join(data_dir, "year")
year_grid_dir = os.path.join(data_dir, "year_grid")
year_interp_dir = os.path.join(data_dir, "year_interp")

if __name__ == '__main__':

    os.makedirs(day_dir, exist_ok=True)
    os.makedirs(month_dir, exist_ok=True)
    os.makedirs(year_dir, exist_ok=True)
    os.makedirs(month_grid_dir, exist_ok=True)
    os.makedirs(month_interp_dir, exist_ok=True)
    os.makedirs(year_grid_dir, exist_ok=True)
    os.makedirs(year_interp_dir, exist_ok=True)

    os.makedirs(os.path.join(raw_dir, "results"), exist_ok=True)


    def drop_existing_files(file_tuples, overwrite=False):
        """Drop file tuples if output file exists and overwrite not True

        Designed to look only at the second item of a tuple because
        all qlists are tuples of length 2 where the second item is the
        output path.
        """
        return [f for f in file_tuples if overwrite or not file_exists(f[1])]


    def output_results(tasks, results, stage):
        """Format and output results from running tasks

        All tasks are tuples with 2 items, where the input path or
        list of paths is the first item and the output path is the second item.
        """
        df = pd.DataFrame(tasks, columns=["input", "output"])

        # ---------
        # column name for join field in original df
        results_join_field_name = "output"
        # position of join field in each tuple in task list
        results_join_field_loc = 1
        # ---------

        # join download function results back to df
        results_df = pd.DataFrame(results, columns=["status", "message", results_join_field_name])
        results_df[results_join_field_name] = results_df[results_join_field_name].apply(lambda x: x[results_join_field_loc])

        output_df = df.merge(results_df, on=results_join_field_name, how="left")

        print("Results:")

        errors_df = output_df[output_df["status"] != 0]
        print("\t{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

        # output results to csv
        output_path = os.path.join(raw_dir, "results", f"data_prepare_{timestamp}_{stage}.csv")
        output_df.to_csv(output_path, index=False)



    # -----------------------------------------------------------------------------
    # prepare daily data

    input_list = glob.glob(os.path.join(raw_dir, "oco2_LtCO2_*.nc4"))
    output_list = [os.path.join(day_dir, "xco2_20{}.csv".format(os.path.basename(i).split("_")[2])) for i in input_list]

    # netcdf daily path, csv daily path
    qlist_a_all = list(zip(input_list, output_list))
    qlist_a = drop_existing_files(qlist_a_all, overwrite=overwrite)

    if run_a:
        results_a = run_tasks(convert_daily, qlist_a, mode, max_workers=max_workers, chunksize=1)
        output_results(qlist_a, results_a, "convert-daily")

    # -----------------------------------------------------------------------------
    # concat all daily for each month


    qlist_b_dict = {}
    for _,i in qlist_a:
        yearmonth = os.path.basename(i).split("_")[1][0:4]
        if yearmonth not in qlist_b_dict:
            qlist_b_dict[yearmonth] = []
        qlist_b_dict[yearmonth].append(i)

    # list of daily csv paths, monthly csv path
    qlist_b_all = [(j, os.path.join(month_dir, "xco2_20{}.csv".format(i))) for i, j in list(qlist_b_dict.items())]
    qlist_b = drop_existing_files(qlist_b_all, overwrite=overwrite)


    if run_b:
        results_b = run_tasks(concat_month, qlist_b, mode, max_workers=max_workers, chunksize=1)
        output_results(qlist_b, results_b, "concat-month")


    # -----------------------------------------------------------------------------
    # concat all month for each year

    mlist = [i[0] for i in qlist_b]

    qlist_c_dict = {}
    for i in mlist:
        year = os.path.basename(i).split("_")[1][0:4]
        if year not in qlist_c_dict:
            qlist_c_dict[year] = []
        qlist_c_dict[year].append(i)

    # list of monthly csv paths, yearly csv path
    qlist_c_all = [(j, os.path.join(year_dir, "xco2_{}.csv".format(i))) for i, j in list(qlist_c_dict.items())]
    qlist_c = drop_existing_files(qlist_c_all, overwrite=overwrite)


    if run_c:
        results_c = run_tasks(concat_year, qlist_c, mode, max_workers=max_workers, chunksize=1)
        output_results(qlist_c, results_c, "concat-year")


    # -----------------------------------------------------------------------------
    # agg monthly data to grid

    # monthly csv path, monthly grid csv path
    qlist_d_all = [(i, i.replace("month", "month_grid")) for i in mlist]
    qlist_d = drop_existing_files(qlist_d_all, overwrite=overwrite)


    if run_d:
        results_d = run_tasks(agg_to_grid_month, qlist_d, mode, max_workers=max_workers, chunksize=1)
        output_results(qlist_d, results_d, "agg-month")


    # -----------------------------------------------------------------------------
    # interpolate month grid data to fill gaps

    # monthly grid csv path, monthly interpolation csv path
    qlist_e_all = [(j, os.path.join(month_interp_dir, "xco2_20{}_{}.tif".format(os.path.basename(j).split("_")[1][0:4], interp_method))) for _, j in qlist_d]
    qlist_e = drop_existing_files(qlist_e_all, overwrite=overwrite)


    if run_e:
        results_e = run_tasks(interpolate_month, qlist_e, mode, max_workers=max_workers, chunksize=1)
        output_results(qlist_e, results_e, "interp-month")


    # -----------------------------------------------------------------------------
    # agg yearly data to grid


    # yearly csv path, yearly grid csv path
    qlist_f_all = [(i[0], i[0].replace("year", "year_grid")) for i in qlist_c]
    qlist_f = drop_existing_files(qlist_f_all, overwrite=overwrite)


    if run_f:
        results_f = run_tasks(agg_to_grid_year, qlist_f, mode, max_workers=max_workers, chunksize=1)
        output_results(qlist_f, results_f, "agg-year")


    # -----------------------------------------------------------------------------
    # interpolate year grid data to fill gaps


    # year grid csv path, year interpolation csv path
    qlist_g_all = [(j, os.path.join(year_interp_dir, "xco2_{}_{}.tif".format(os.path.basename(j).split("_")[1][0:4], interp_method))) for _, j in qlist_f]
    qlist_g = drop_existing_files(qlist_g_all, overwrite=overwrite)


    if run_g:
        results_g = run_tasks(interpolate_year, qlist_g, mode, max_workers=max_workers, chunksize=1)
        output_results(qlist_g, results_g, "interp-year")
