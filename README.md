# metriql Tableau Integration

Generates Tableau Data Source ([TDS](https://help.tableau.com/current/pro/desktop/en-us/environ_filesandfolders.htm#content-body)) files from your metriql datasets.
The idea is to leverage metriql datasets in your Tableau workflow without any additional modeling in Tableau.

### Usage

The library is available in PyPI so you can install it via pip as follows:

```
pip install metriql-superset
```

The library expects `stdin` for the metriql metadata and outputs a TDS file to `stdout`. Here is an example:

```
curl http://metriql-server.com/api/v0/metadata | metriql-superset --metriql-url http://metriql-server.com --dataset your_dataset create-tds > your_dataset.tds
```

You can use `--file` argument instead of reading the metadata from `stdin` as an alternative.
You can use `--out` argument to create a file instead of printing the TDS file to `stdout` as an alternative.

The only command is `create-tds` for now.

### How does it work?

The generated file includes your metriql URL and uses Presto interface which is natively supported in Tableau. In order to use Tableau integration, you need to enable JDBC in your metriql server.
