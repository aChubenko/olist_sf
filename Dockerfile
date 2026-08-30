FROM astrocrpublic.azurecr.io/runtime:3.3-2-python-3.13

ENV AIRFLOW__COSMOS__ENABLE_TELEMETRY=false \
    AIRFLOW__COSMOS__ENABLE_CACHE=false

# Astro Runtime installs requirements.txt and packages.txt during the image build.
# These project assets are copied explicitly because DAG tasks execute them at runtime.
COPY --chown=astro:0 dbt /usr/local/airflow/dbt
COPY --chown=astro:0 scripts /usr/local/airflow/scripts
COPY --chown=astro:0 data /usr/local/airflow/data
