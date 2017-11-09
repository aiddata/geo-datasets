import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import time
from adjustText import adjust_text, repel_text
import plotly.graph_objs as go
import plotly.plotly as py

# -*- coding: utf-8 -*-

# cd Desktop first
file = r"/Users/miranda/Documents/AidData/datasets/AFB/rasterization/Merged Stacked Datasets_v3.dta"

# ------------------------------------
# create output directory

dirc = os.path.dirname(file)

for rd in range(1, 7):
    folder = "round_" + str(rd)
    fpath = os.path.join(dirc, folder)
    if not os.path.isdir(fpath):
        os.mkdir(fpath)

dta = pd.read_stata(file)

# ------------------------------------------
# Set filters: each questions per round
questions = ["trust_pres", "trust_police", "trust_court", "trust_electcom",
            "trust_party", "trust_oppart"]

# ---------------------------------
# calculate percentage rate of fine locations within a given round

def percentage_fine(gdf, question):
    gdf['country_name'] = gdf['geoname_adm_name'].apply(lambda x: x.split('|')[2])
    countrynames = list(gdf['country_name'].unique())
    categorylist = ["fine", "coarse"]

    wiredcase = {"Republic of Mauritius": "Mauritius", "Western Sahara": "Morocco"}

    gdf['country_name'] = gdf['country_name'].apply(
        lambda x: wiredcase[x] if x.decode('utf-8') in wiredcase.keys() else x)

    # gdf['country_name'] = gdf[gdf['country_name']=='Republic of Mauritius']
    gdf_group = gdf.groupby(['country_name', 'category'])

    dta_dict = dict()
    dta_dict["country_name"] = list()
    dta_dict["fine_count"] = list()
    dta_dict["coarse_count"] = list()
    dta_dict["fine_mean"] = list()
    dta_dict["coarse_mean"] = list()

    for countryname in countrynames:

        dta_dict["country_name"].append(countryname)

        for category in categorylist:

            count_name = category + "_count"
            mean_name = category + "_mean"

            if (countryname, category) in gdf_group.groups.keys():

                dta_dict[count_name].append(len(gdf_group.get_group((countryname, category))))
                dta_dict[mean_name].append(gdf_group.get_group((countryname, category))[question].mean())
            else:
                dta_dict[count_name].append(0)
                dta_dict[mean_name].append(0)

    newdf = pd.DataFrame.from_dict(dta_dict)

    newdf['fine_count_percent'] = newdf['fine_count'] / (newdf['fine_count'] + newdf['coarse_count'])
    newdf['coarse_count_percent'] = newdf['coarse_count'] / (newdf['fine_count'] + newdf['coarse_count'])
    newdf['abs_mean_diff'] = newdf['fine_mean'] / (newdf['fine_mean'] + newdf['coarse_mean'])

    return newdf


def plot_map(dta, mapvar, maptitle, legendtitle, outfile):
    try:
        py.sign_in('mirandalv', 'TMvHv1KBIfeambeSCXwt')
    except:
        time.sleep(60)
        py.sign_in('mirandalv', 'TMvHv1KBIfeambeSCXwt')

    data = [go.Choropleth(
        type='choropleth',
        autocolorscale=True,
        locations=dta['country_name'],
        z=dta[mapvar].astype(float),
        locationmode='country names',
        marker=dict(
            line=dict(
                color='rgb(255,255,255)',
                width=2
            )),
        colorbar=dict(
            title=legendtitle,
            ticklen=3,
            len=0.5,
            thickness=10)
    )]

    layout = dict(
        title=maptitle,
        geo=dict(
            scope='africa',
            projection=dict(type='Mercator'),
            showlakes=True,
            lakecolor='rgb(255, 255, 255)'),
        showlegend=True
    )

    fig = dict(data=data, layout=layout)
    # py.plot(fig, filename='d3-cloropleth-map.html')
    py.image.save_as(fig, filename=outfile, scale=10)


def scatter_label(df, outf, rd):
    x = df['coarse_count_percent']
    y = df['fine_count_percent']

    labels = df['country_name']

    plt.scatter(x, y, s=2)

    texts = list()

    for label, x, y in zip(labels, x, y):
        # plt.annotate(label.decode('utf-8'), xy=(x, y))
        label = label.decode('utf-8')
        texts.append(plt.text(x, y, label))

    # force_text: r1 = 1.0; r2=1.0; r3=1.0; r4=; r5=1.6; r6=

    adjust_text(texts, force_text=0.8, autoalign='y', precision=1,
                arrowprops=dict(arrowstyle="-|>", color='r', alpha=0.3, lw=0.5))

    plt.xlabel('Coarse location percentage')
    plt.ylabel('Fine location percentage')

    title = "Location accuracy of each country in round: " + str(rd)
    plt.title(title)
    plt.savefig(outf, dpi=900)


# --------------------------------------------
# define precision categories

is_precise = ((dta["location_class"] == 2) | (dta["location_class"] == 3) | (
(dta["location_class"] == 1) & (dta["location_type_code"].isin(["ADM3", "ADM4", "ADM4H", "ADM5"]))))
dta['category'] = None

dta.loc[is_precise, 'category'] = 'fine'
dta.loc[~is_precise, 'category'] = 'coarse'

gdf = dta.copy(deep=True)

for rd in range(1, 7, 1):

    print "Start working on Round: ", str(rd)

    # set path
    folder = "round_" + str(rd)
    fpath = os.path.join(dirc, folder)

    dta_gdf = gdf.loc[gdf['round'] == rd]

    for question in questions:

        if not dta_gdf[question].isnull().all():
            print "Working on Round " + str(rd) + " Question " + question
            print "---------------------------------------------------"

            # --------------------------------- Start plot -----------------------------------

            print "Working on category plot..........."
            plt.clf()
            dta_asub = dta_gdf.loc[dta_gdf['category'] == 'fine']
            num = "N = " + str(len(dta_asub))
            plt.plot(dta_asub["longitude"], dta_asub["latitude"], 'r.', markersize=2)
            name = "Category_Fine: " + question + "_" + str(rd)
            plt.title(name)
            name = "r_" + str(rd) + "_scattorplot_a_" + question
            plt.savefig(os.path.join(os.path.join(fpath, name)))

            plt.clf()
            dta_bsub = dta_gdf.loc[dta_gdf['category'] == 'coarse']
            num = "N = " + str(len(dta_bsub))
            plt.plot(dta_bsub["longitude"], dta_bsub["latitude"], 'X', markersize=2)
            name = "Category_B: " + question + "_" + str(rd)
            plt.title(name)
            name = "r_" + str(rd) + "_scattorplot_b_" + question
            plt.savefig(os.path.join(fpath, name))
            plt.clf()

            summary_df = percentage_fine(dta_gdf, question)

            # map 1: percentage of fine locations (this is identical for different question
            # Only one plot each round)

            print "Working on percentage of fine locations..........."

            maptitle1 = "Percentage of coding at fine level - " + "Round " + str(rd)
            legendtitle1 = "values scale at 1"
            outname1 = "round_" + str(rd) + "fine_percent.png"
            outfile1 = os.path.join(fpath, outname1)
            mapvar1 = 'fine_count_percent'

            if not os.path.exists(outfile1):
                plot_map(summary_df, mapvar1, maptitle1, legendtitle1, outfile1)


            # map2: category histogram

            print "Working on histogram..........."
            plt.clf()
            plot = sns.countplot(x=question, hue="category", data=dta_gdf)
            fig = plot.get_figure()
            name = question + "_round_" + str(rd)
            plt.title(name)
            fig.savefig(os.path.join(fpath, name))

            # map3: Absolute difference of mean(A) & mean(B)
            print "Working on mean difference..........."
            plt.clf()

            maptitle3 = "Round - " + str(rd) + " - Absolute difference between fine/coarse coding - " + question
            legendtitle3 = "values scale at 1"
            outname3 = "round_" + str(rd) + "absolute difference " + question + ".png"
            outfile3 = os.path.join(fpath, outname3)
            mapvar3 = 'abs_mean_diff'
            plot_map(summary_df, mapvar3, maptitle3, legendtitle3, outfile3)

            # map4: fine & coarse plot for each country (this is identical for different question
            # Only one plot each round)

            print "Working on scatterplots..........."
            plt.clf()
            mapvar4 = 'Round ' + str(rd) + ' plot.png'
            #if not os.path.exists(os.path.join(fpath, mapvar4)):
            scatter_label(summary_df, os.path.join(fpath, mapvar4), rd)
            


