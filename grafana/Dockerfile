FROM grafana/grafana-oss:10.4.3

# Install the Cassandra datasource plugin
RUN grafana-cli plugins install hadesarchitect-cassandra-datasource

COPY grafana.ini /etc/grafana/grafana.ini
COPY provisioning /etc/grafana/provisioning
COPY dashboards /var/lib/grafana/dashboards
