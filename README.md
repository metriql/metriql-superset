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

