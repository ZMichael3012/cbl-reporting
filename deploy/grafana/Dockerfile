# Grafana with the official ClickHouse datasource plugin baked in (CBL-11.5).
# Versions are pinned (CBL-002, section 2: all images are pinned by version); the
# plugin requires Grafana >= 11.6.0-0. No plugin downloads at runtime: the stand
# stays offline and reproducible.
FROM grafana/grafana:11.6.0

USER root
RUN grafana cli --pluginsDir "$GF_PATHS_PLUGINS" plugins install grafana-clickhouse-datasource 4.22.1 \
    && chown -R grafana:root "$GF_PATHS_PLUGINS"
USER grafana
