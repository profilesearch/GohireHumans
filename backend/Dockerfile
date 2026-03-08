FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create /data fallback (Railway volume replaces this at runtime)
RUN mkdir -p /data

# Make start script executable
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Expose port (Railway sets PORT env var)
EXPOSE 8080

# Start script fixes volume permissions at runtime, then launches gunicorn
CMD ["/app/start.sh"]
