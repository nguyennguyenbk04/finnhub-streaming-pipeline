FROM apache/superset:4.0.0
USER root
RUN pip install --no-cache-dir trino sqlalchemy-trino
COPY docker-init.sh /docker-init.sh
RUN chmod +x /docker-init.sh
USER superset
ENTRYPOINT ["/docker-init.sh"]
