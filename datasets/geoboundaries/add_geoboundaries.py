"""
utility func to add geoboundaries dataset to geo:
    - build dataset specific options
    - generate metadata for dataset resources
    - create document
    - update mongo database
"""

import sys
import os
import datetime
import json
from warnings import warn

# from unidecode import unidecode

utils_dir = "/sciclone/aiddata10/geo/master/source/geo-hpc/utils"
# utils_dir = os.path.join(
#     os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_dir)

import ingest_resources as ru
from ingest_database import MongoUpdate


def run(path=None, client=None, version=None, config=None,
        generator="auto", update=False, dry_run=False):

    print('\n---------------------------------------')

    script = os.path.basename(__file__)

    def quit(reason):
        """quit script cleanly

        to do:
        - do error log stuff
        - output error logs somewhere
        - if auto, move job file to error location
        """
        raise Exception("{0}: terminating script - {1}\n".format(
            script, reason))


    if config is not None:
        client = config.client
    elif client is not None:
        config = client.info.config.findOne()
    else:
        quit('Neither config nor client provided.')


    # update mongo class instance
    dbu = MongoUpdate(client)

    # -------------------------------------


    # check path
    if path is not None:
        if not os.path.exists(path):
            quit("Invalid path provided.")
    else:
        quit("No path provided")

    # optional arg - mainly for user to specify manual run
    if generator not in ['auto', 'manual']:
        quit("Invalid generator input")

    if client is None:
        quit("No mongodb client connection provided")

    if config is None:
        quit("No config object provided")


    raw_update = update
    if update in ["partial", "meta"]:
        update = "partial"
    elif update in ["update", True, 1, "True", "full", "all"]:
        update = "full"
    elif update in ["missing"]:
        update = "missing"
    else:
        update = False

    print("running update status `{0}` (input: `{1}`)".format(
        update, raw_update))

    if dry_run in ["false", "False", "0", "None", "none", "no"]:
        dry_run = False

    dry_run = bool(dry_run)
    if dry_run:
        print("running dry run")

    base_original = client.asdf.data.find_one({'base': path})

    existing_original = None
    if update:
        if not "data" in client.asdf.collection_names():
            update = False
            msg = "Update specified but no data collection exists."
            if generator == "manual":
                raise Exception(msg)
            else:
                warn(msg)
        else:
            if base_original is None and update != "missing":
                update = False
                msg = "Update specified but no existing dataset found."
                if generator == "manual":
                    raise Exception(msg)
                else:
                    warn(msg)


    # init document
    doc = {}

    doc["asdf"] = {}
    doc["asdf"]["script"] = script
    doc["asdf"]["version"] = version
    doc["asdf"]["generator"] = generator
    doc["asdf"]["date_updated"] = str(datetime.date.today())
    if not update or update == "missing":
        doc["asdf"]["date_added"] = str(datetime.date.today())

    # -------------------------------------

    if os.path.isdir(path):
        # remove trailing slash from path
        if path.endswith("/"):
            path = path[:-1]
    else:
        quit("Invalid base directory provided.")

    # -------------------------------------

    doc['base'] = path

    doc["type"] = "boundary"
    doc["file_format"] = "vector"
    doc["file_extension"] = "geojson"
    doc["file_mask"] = "None"


    # -------------------------------------

    metadata_path = os.path.join(path, 'metadata.json')
    metadata = json.load(open(metadata_path, 'r'))


    name = os.path.basename(doc["base"])

    iso3 = metadata["boundaryISO"]
    adm = metadata["boundaryType"]

    country = metadata["boundaryName"]


    doc["name"] = iso3.lower() + "_" + adm.lower() + "_gb_" + version


    inactive_bnds_list = config.inactive_bnds
    is_active = doc["name"] not in inactive_bnds_list

    doc["active"] = int(is_active)


    name_original = client.asdf.data.find_one({'name': doc["name"]})

    if not update and base_original is not None:
        msg = "No update specified but dataset exists (base: {0})".format(base_original['base'])
        raise Exception(msg)
    elif not update and name_original is not None:
        msg = "No update specified but dataset exists (name: {0})".format(name_original['name'])
        raise Exception(msg)


    if update:

        if update == "missing" and name_original is not None and base_original is not None:
            warn("Dataset exists (running in 'missing' update mode). Running partial update and setting to active (if possible).")
            update = "partial"

        if update != "missing":
            if name_original is None and base_original is None:
                update = False
                warn(("Update specified but no dataset with matching "
                      "base ({0}) or name ({1}) was found").format(doc["base"],
                                                                   doc["name"]))

                # in case we ended up not finding a match for name
                doc["asdf"]["date_added"] = str(datetime.date.today())

            elif name_original is not None and base_original is not None:

                if str(name_original['_id']) != str(base_original['_id']):
                    quit("Update option specified but identifying fields (base "
                         "and name) belong to different existing datasets."
                         "\n\tBase: {0}\n\tName: {1}".format(doc["base"],
                                                             doc["name"]))
                else:
                    existing_original = name_original

            elif name_original is not None:
                existing_original = name_original

            elif base_original is not None:
                existing_original = base_original


            doc["asdf"]["date_added"] = existing_original["asdf"]["date_added"]

            if existing_original["active"] == -1:
                doc["active"] = -1


    doc["title"] = "{} {} - GeoBoundaries {}".format(country, adm.upper(), version.replace("_", "."))

    doc["description"] = metadata["boundaryID"]

    doc["version"] = version


    doc["options"] = {}
    doc["options"]["group"] = iso3.lower() + "_gb_" + version
    doc["options"]["group_title"] = "{} - GeoBoundaries {}".format(country, version.replace("_", "."))


    doc["extras"] = {}

    doc["extras"]["citation"] = ('Runfola, Daniel, Austin Anderson, Heather Baier, Matt Crittenden, Elizabeth Dowker, Sydney Fuhrig, Seth Goodman, Grace Grimsley, Rachel Layko, Graham Melville, Maddy Mulder, Rachel Oberman, Joshua Panganiban, Andrew Peck, Leigh Seitz, Sylvia Shea, Hannah Slevin, Rebecca Yougerman, Lauren Hobbs. "geoBoundaries: A global database of political administrative boundaries." Plos one 15, no. 4 (2020): e0231866.')


    doc["extras"]["sources_web"] = "http://www.geoboundaries.org"
    doc["extras"]["sources_name"] = "geoBoundaries"

    doc["extras"]["country"] = country
    doc["extras"]["iso3"] = iso3
    doc["extras"]["adm"] = int(adm[-1:])

    doc["extras"]["tags"] = ["geoboundaries", adm, country, iso3]

    doc["extras"]["original_metadata_url"] = metadata["apiURL"]


    # boundary group
    if "adm0" in name.lower():
        doc["options"]["group_class"] = "actual"
        doc["active"] = -1
    else:
        doc["options"]["group_class"] = "sub"

    # -------------------------------------
    # resource scan

    # find all files with file_extension in path
    file_list = []
    for root, dirs, files in os.walk(doc["base"]):
        for fname in files:

            fname = os.path.join(root, fname)
            file_check = fname.endswith('.' + doc["file_extension"])

            if file_check == True and not fname.endswith('simplified.geojson'):
                file_list.append(fname)


    if len(file_list) == 0:
        quit("No vector file found in " + doc["base"])

    elif len(file_list) > 1:
        quit("Boundaries must be submitted individually.")


    f = file_list[0]
    print(f)


    doc["description"] = "GeoBoundaries boundary file for {} in {}.".format(
        adm.upper(), country)


    # -------------------------------------

    if update == "partial":
        print("\nProcessed document:")
        print(doc)

        print("\nUpdating database (dry run = {0})...".format(dry_run))
        if not dry_run:
            dbu.update(doc, update, existing_original)

        print("\n{0}: Done ({1} update).\n".format(script, update))
        return 0

    # -------------------------------------
    print("\nProcessing temporal...")

    # temporally invariant dataset
    doc["temporal"] = {}
    doc["temporal"]["name"] = "Temporally Invariant"
    doc["temporal"]["format"] = "None"
    doc["temporal"]["type"] = "None"
    doc["temporal"]["start"] = 10000101
    doc["temporal"]["end"] = 99991231

    # -------------------------------------
    print("\nProcessing spatial...")

    if not dry_run:
        convert_status = ru.add_asdf_id(f)
        if convert_status == 1:
             quit("Error adding ad_id to boundary file & outputting geojson.")


    env = ru.vector_envelope(f)
    env = ru.trim_envelope(env)
    print("Dataset bounding box: ", env)

    doc["scale"] = ru.envelope_to_scale(env)

    # set spatial
    doc["spatial"] = ru.envelope_to_geom(env)

    # -------------------------------------
    print('\nProcessing resources...')

    # resources
    # individual resource info
    resource_tmp = {}

    # path relative to base
    resource_tmp["path"] = f[f.index(doc["base"]) + len(doc["base"]) + 1:]

    resource_tmp["name"] = doc["name"]
    resource_tmp["bytes"] = os.path.getsize(f)
    resource_tmp["start"] = 10000101
    resource_tmp["end"] = 99991231

    # reorder resource fields
    # resource_order = ["name", "path", "bytes", "start", "end"]
    # resource_tmp = OrderedDict((k, resource_tmp[k]) for k in resource_order)

    # update main list
    resource_list = [resource_tmp]

    doc["resources"] = resource_list

    # -------------------------------------
    # database updates

    print("\nProcessed document:")
    print(doc)

    print("\nUpdating database (dry run = {0})...".format(dry_run))
    if not dry_run:
        dbu.update(doc, update, existing_original)
        # try:
        #     dbu.features_to_mongo(doc['name'])
        # except:
        #     # could remove data entry if it cannot be added
        #     # to mongo. or, at least make sure the data entry is
        #     # set to inactive
        #     raise

    if update:
        print("\n{0}: Done ({1} update).\n".format(script, update))
    else:
        print("\n{0}: Done.\n".format(script))

    print('\n---------------------------------------\n')

    return 0


# -----------------------------------------------------------------------------

if __name__ == '__main__':

    # if calling script directly, use following input args:
    #   branch (required)
    #   path (absolute) to release (required)
    #   generator (optional, defaults to "manual")
    #   update (bool)

    branch = sys.argv[1]

    from config_utility import BranchConfig

    config = BranchConfig(branch=branch)

    # check mongodb connection
    if config.connection_status != 0:
        raise Exception("connection status error: {0}".format(
            config.connection_error))


    # -------------------------------------


    path = sys.argv[2]

    version = os.path.basename(os.path.dirname(path))

    generator = sys.argv[3]

    if len(sys.argv) >= 5:
        update = sys.argv[4]
    else:
        update = False

    if len(sys.argv) >= 6:
        dry_run = sys.argv[5]
    else:
        dry_run = False

    run(path=path, version=version, config=config, generator=generator,
        update=update, dry_run=dry_run)
