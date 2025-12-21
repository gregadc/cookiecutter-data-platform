FROM apache/superset:latest

USER root

# Installe psycopg2 globalement
RUN pip install --no-cache-dir psycopg2-binary

# Copie aussi dans le site-packages de superset
RUN cp -r /usr/local/lib/python3.10/site-packages/psycopg2* /app/pythonpath/ || true

USER superset

ENV SUPERSET_SECRET_KEY=your_secret_key_change_this_in_production

RUN superset db upgrade && \
    superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@superset.com \
        --password admin && \
    superset init