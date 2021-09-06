# metriql Superset Integration

Synchronize Superset datasets from Metriql datasets. The idea is to leverage metriql datasets in your Tableau workflow without any additional modeling in Tableau.

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

Not yet because Preset has its own authentication method using Auth0. Contributions are welcomed though! 

