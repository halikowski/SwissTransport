# Install python 3.11 (max.), because Snowpark requires python version < 3.12
FROM python:3.11

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    xvfb \
    libxss1 \
    libgconf-2-4 \
    libnss3 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libdrm-common \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libgtk-3-common \
    libxcomposite1 \
    libxdamage1 \
    libxkbcommon0 \
    libxrandr2 \
    libxtst6 \
    gnupg \
    ca-certificates

# Add Google Chrome repository key
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add -

# Add Google Chrome repository to sources list
RUN echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list

# Update package lists again
RUN apt-get update

# Install Google Chrome
RUN apt-get install -y google-chrome-stable

# Download and install SnowSQL
RUN curl -o snowsql-1.2.32-linux_x86_64.bash https://sfc-repo.azure.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.32-linux_x86_64.bash \
    && chmod +x snowsql-1.2.32-linux_x86_64.bash \
    && SNOWSQL_DEST=/usr/local/bin SNOWSQL_LOGIN_SHELL=/etc/profile bash snowsql-1.2.32-linux_x86_64.bash \
    && rm -f snowsql-1.2.32-linux_x86_64.bash

# Ensure SnowSQL is properly installed
RUN snowsql -v

# Environment variables setup
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=${PYTHONPATH}:/opt/airflow/scripts

# Install Apache Airflow
RUN pip install apache-airflow

# Set a default Chrome version
ENV DEFAULT_CHROME_VERSION=114.0.5735.90

# Specify the version of Chromedriver
ENV CHROMEDRIVER_VERSION=94.0.4606.61

# Download and install Chromedriver
RUN wget -O /tmp/chromedriver.zip "https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip" \
    && unzip /tmp/chromedriver.zip -d /usr/local/bin/ \
    && rm /tmp/chromedriver.zip

# Copy necessary files
COPY dags/ $AIRFLOW_HOME/dags/
COPY config/ $AIRFLOW_HOME/config/
COPY scripts/ $AIRFLOW_HOME/scripts/
COPY docker-entrypoint.sh /entrypoint.sh
COPY requirements.txt .
COPY .env /opt/airflow/.env

# Ensure entrypoint script is executable
RUN chmod +x /entrypoint.sh

# Install additional Python dependencies from requirements file
RUN pip install -r requirements.txt

# Set entrypoint and default command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
