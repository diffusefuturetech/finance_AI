FROM python:3.11-slim

# Install system deps for TA-Lib and Chinese fonts
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    libta-lib0-dev \
    fonts-wqy-zenhei \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Install TA-Lib C library if not available via apt
RUN if ! ldconfig -p | grep -q libta_lib; then \
        wget -q http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
        tar -xzf ta-lib-0.4.0-src.tar.gz && \
        cd ta-lib && ./configure --prefix=/usr && make -j$(nproc) && make install && \
        cd .. && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz && ldconfig; \
    fi

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create required directories
RUN mkdir -p /root/.qlib/qlib_data/cn_data_finance_ai/{calendars,instruments,features} \
    && mkdir -p /root/.finance_ai \
    && mkdir -p /tmp/finance_ai_charts \
    && mkdir -p /tmp/finance_ai_staging

# Persistent data volumes
VOLUME ["/root/.qlib", "/root/.finance_ai", "/tmp/finance_ai_charts"]

# Setup cron jobs for daily tasks (15:30 market summary, 16:00 data update)
RUN echo '30 15 * * 1-5 cd /app && python3 -c "from skill.scheduler import DailyScheduler; DailyScheduler().daily_market_summary()" >> /var/log/cron.log 2>&1' > /etc/cron.d/finance-ai \
    && echo '0 16 * * 1-5 cd /app && python3 scripts/update_data.py >> /var/log/cron.log 2>&1' >> /etc/cron.d/finance-ai \
    && chmod 0644 /etc/cron.d/finance-ai \
    && crontab /etc/cron.d/finance-ai \
    && touch /var/log/cron.log

ENV TZ=Asia/Shanghai
ENV PYTHONPATH=/app

# Default: start scheduler (or use CMD override for one-off commands)
CMD ["python3", "skill/scheduler.py"]
