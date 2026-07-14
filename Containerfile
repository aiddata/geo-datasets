# syntax=docker/dockerfile:1
#
# Environment image for running geo-datasets Prefect flows (one per dataset).
#
# This image carries ONLY the dependency environment (pyproject.toml / uv.lock
# + the local data_manager package). It deliberately does NOT bake in the
# dataset code under datasets/: at flow-run time the deployment's git pull step
# (flow.from_source(GitRepository(...)) in scripts/deploy.py) clones the repo
# fresh into the job pod and runs from that clone. So rebuild this image only
# when the dependencies change, not when dataset code changes.
#
# Build from the repository root:
#
#   podman build -t geodata-jobs -f Containerfile .
#
# All of the geospatial deps (fiona, rasterio, geopandas, pyhdf, netcdf4, ...)
# resolve to wheels in uv.lock, and those wheels vendor their own GDAL / GEOS /
# PROJ / HDF. That means no system GDAL/HDF dev packages are required here.

# The Python version is NOT pinned here: it is read from .python-version so that
# file stays the single source of truth for the whole project. Rather than base
# on a python:X.Y image (which would bake the version into this Containerfile),
# we start from plain debian-slim and let uv install the interpreter that
# .python-version requests (uv sync reads it automatically).
FROM debian:bookworm-slim

# Pin uv to the same version used to author uv.lock for reproducible installs.
COPY --from=ghcr.io/astral-sh/uv:0.10.2 /uv /uvx /bin/

# Runtime system libraries:
#  - libexpat1: needed by the GDAL bundled inside the fiona/rasterio wheels
#  - ca-certificates: TLS for flows that fetch data over HTTPS
#  - git, wget: commonly invoked by dataset download/processing steps
RUN apt-get update && apt-get install -y --no-install-recommends \
        libexpat1 \
        ca-certificates \
        git \
        wget \
    && rm -rf /var/lib/apt/lists/*

# UV_PYTHON is intentionally unset so it does not override the .python-version
# request; UV_PYTHON_DOWNLOADS=automatic lets uv fetch that interpreter.
#
# UV_PYTHON_INSTALL_DIR moves that interpreter out of /root, which is mode 0700.
# Flow pods run as the SciClone uid rather than root (see the work pool base job
# template), and .venv/bin/python is a symlink to the uv-managed interpreter, so
# leaving it under /root makes the venv unusable to every non-root user:
# "exec .venv/bin/python: Permission denied".
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=automatic \
    UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install the locked dependencies plus the local data_manager package. Only the
# files needed for that are copied in, so the build context (and what can bust
# this layer) is limited to the things that actually define the environment.
# .python-version selects the interpreter uv installs and builds the venv with.
# The root geo-datasets project is virtual (no [build-system]), so uv installs
# its dependencies without trying to build it; --no-install-project makes that
# explicit. data_manager is a path dependency, so its source must be present.
COPY .python-version pyproject.toml uv.lock README.md ./
COPY data_manager/ ./data_manager/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv python install \
    && uv sync --frozen --no-install-project --no-dev

# Put the project venv first on PATH so `python`, `prefect`, etc. resolve to it.
ENV PATH="/app/.venv/bin:$PATH"

# Flow pods run as a uid that has no entry in /etc/passwd, so HOME would
# otherwise resolve to the root-owned WORKDIR and Prefect could not create its
# home directory. Point both at a world-writable location.
ENV HOME=/tmp \
    PREFECT_HOME=/tmp/.prefect

# Default command. When this image is used as a Prefect Kubernetes work-pool job
# image, Prefect overrides the command to execute the flow run; this CMD is the
# fallback when the container is run directly (e.g. to start a worker locally).
CMD ["prefect", "worker", "start", "--pool", "geodata-pool"]
