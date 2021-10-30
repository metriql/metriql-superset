# Metriql Superset Integration

Synchronize Superset datasets from Metriql datasets. The idea is to leverage Metriql datasets in your Superset workflow without any additional modeling in Superset.

### Usage

The library is available in PyPI so you can install it via pip as follows:

```
pip install metriql-superset
```

The library expects `stdin` for the metriql metadata and interacts with Superset via session tokens. Here is an example:

```
curl http://metriql-server.com/api/v0/metadata | metriql-superset --metriql-url http://metriql-server.com --superset-username USERNAME --superset-password PASSWORD sync-database
```

You can use `--file` argument instead of reading the metadata from `stdin` as an alternative.

Available commands are `create-database`, `list-databases`, `sync-database`.

### FAQ

#### Why don't you use Superset API?

While Superset provides an API, it doesn't let updating / creating datasets and databases. Therefore, we create session token using internal APIs and interact with the internal APIs.

#### Do you support Preset Cloud?

Yes! Preset offers [an API](https://docs.preset.io/docs/using-the-preset-api) in their Enterprise Plan and you can use the API tokens to be able to synchronize the metrics of Superset deployments in your Preset account. The `token` in the picture below should be passed as `--superset-username` and the value of `secret` should be passed as `--superset-password`:

![Preset API Token](/preset-token-image.png)

#### Why do I need to run a separete service to use metriql-superset?

Metriql requires you to run a separate service to be able to access the data in your data warehouse. In some cases, it might be overhead and we would love to get contributions from the community to make metriql-superset compatible with dbt metrics once they're released. However; here are the benefits of running Metriql as a separate service:

1. Superset doesn't support JOIN relations in query builder since Metriql exposes all the fields including the ones that come from the [relations](https://metriql.com/reference/relation) as database columns, you will be able to unlock that feature in Superset.
2. Metriql has [Aggregates](https://metriql.com/introduction/aggregates) which speeds up OLAP queries by pre-aggregating the fact tables. It re-writes the queries Superset runs on Metriql before executing them in your data warehouse.
4. Metriql has an advanced semantic layer that lets you create [filtered measures](https://metriql.com/reference/measure#filters), [non-additive](https://metriql.com/reference/measure#aggregation) and [window](https://metriql.com/reference/measure#window) measures which are not available in Superset. Additionally, you can use [Jinja expressions](https://metriql.com/reference/sql-context) in your metrics/dimensions.
5. Superset has timeframes to some extent but Metriql offers a set of different [timeframes](https://metriql.com/reference/dimension#timeframes) that lets you analyze the data in different granularities.
6. It's not possible to access the metrics created in Superset from a different BI tool / SQL client but Metriql lets you access your metrics from different [data tools](https://metriql.com/integrations/bi-tools/index).
