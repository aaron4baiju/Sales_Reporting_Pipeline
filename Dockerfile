# Use a lightweight Python base image
FROM python:3.10-slim

# Set working directory in the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy all project files into the container
COPY . .

# Copy the config folder into the image
COPY Config /app/Config

# Set default command to run your ETL script
CMD ["python", "Scripts/etl_main.py"]