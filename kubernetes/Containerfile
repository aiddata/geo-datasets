FROM prefecthq/prefect:2.7.3-python3.8

# create workdir
WORKDIR /app

# build custom environment
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# copy the rest of the directory
COPY . .

# run program
CMD ["python", "malaria_atlas_project/main.py"]
