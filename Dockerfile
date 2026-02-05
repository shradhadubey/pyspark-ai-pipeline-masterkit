FROM bitnami/spark:3.5.0

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/

# Set the default command
CMD ["python", "src/transform.py"]