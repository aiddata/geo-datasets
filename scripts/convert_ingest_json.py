"""
Convert a legacy ingest JSON to the current schema (reference:
datasets/esa_landcover/raster_ingest.json).

Mechanical mapping:
- active/public: legacy ints -> bools (public defaults true)
- path: /data/datasets/<ingest name>
- options.extract_types (+ extract_types_info) -> processing_options entries
  using the rasterstats_default_* functions from the GeoQuery backend;
  `count` is public=false (internal QA stat), everything else public=true
- options.resolution -> other.resolution; options.factor -> variable_factor;
  options.variable_description -> variable_description
- extras.{citation,sources_web,sources_name,tags} hoisted to top level
- extras.category_map -> mapped: true + mappings
- dropped: base, version, options, extras

Content fields (descriptions, is_global, kwargs like nodata) still need human
review after conversion.

Usage:
    python scripts/convert_ingest_json.py <path/to/legacy_ingest.json> [more...]
Converts in place.
"""

import json
import sys

RESULT_TYPES = {"categorical": "int", "count": "int"}


def convert(path: str) -> None:
    with open(path) as f:
        old = json.load(f)

    if "options" not in old and "extras" not in old:
        print(f"SKIP (already converted?): {path}")
        return

    options = old.get("options", {})
    extras = old.get("extras", {})

    processing_options = []
    info = options.get("extract_types_info", {})
    for et in options.get("extract_types", []):
        processing_options.append(
            {
                "function": f"rasterstats_default_{et}",
                "short_name": et,
                "description": info.get(et, ""),
                "kwargs": {},
                "active": True,
                "public": et != "count",
                "result_type": RESULT_TYPES.get(et, "float"),
            }
        )

    category_map = extras.get("category_map", {})

    new = {
        "active": bool(old.get("active", True)),
        "public": bool(old.get("public", True)),
        "is_global": bool(old.get("is_global", True)),
        "path": f"/data/datasets/{old['name']}",
        "type": old.get("type", "raster"),
        "file_extension": old.get("file_extension", ".tif"),
        "file_mask": old.get("file_mask", "None"),
        "name": old["name"],
        "short_name": old.get("short_name", old.get("title", old["name"])),
        "title": old.get("title", ""),
        "description": old.get("description", ""),
        "details": old.get("details", ""),
        "processing_class": "zonal_stats",
        "processing_options": processing_options,
        "other": {"resolution": options["resolution"]} if "resolution" in options else {},
        "variable_factor": options.get("factor", 1),
        "variable_description": options.get("variable_description", ""),
        "mapped": bool(category_map),
        "mappings": category_map,
        "citation": extras.get("citation", ""),
        "sources_web": extras.get("sources_web", ""),
        "sources_name": extras.get("sources_name", ""),
        "tags": extras.get("tags", []),
        "ingest_src": None,
        "coverage_dependency": None,
    }

    with open(path, "w") as f:
        json.dump(new, f, indent=4)
        f.write("\n")
    print(f"converted: {path}")


if __name__ == "__main__":
    for p in sys.argv[1:]:
        convert(p)
