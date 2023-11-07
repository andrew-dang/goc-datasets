FROM prefecthq/prefect:2-python3.11

# Copy pip install requirements
COPY requirements.txt .

# Install python packages
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir