# Avro-to-Arrow

This package provides a fast path for converting from Avro (as serialized by
the BigQuery Storage API) to the Arrow Table in-memory format for fast
analytics.

This package is created for educational / experimental purposes.

## About the fork

This project is a fork of the [Apache Avro™](https://github.com/apache/avro) project.

Learn more about Avro, please visit their website at:

  http://avro.apache.org/

This package optimizes Avro parsing of the schemaless blocks provided by the
BigQuery Storage API. Many of these optimatizations don't make sense in a
general purpose parser package.

## Contributing

This package is made for educational purposes. Direct your efforts towards the
official Arrow and Avro packages, instead.

## License

Apache Version 2.0

