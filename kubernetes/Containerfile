FROM prefecthq/prefect:2.7.3-python3.8

# build custom environment
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# run program
CMD ["prefect", "agent", "start", "-q", "geodata"]
