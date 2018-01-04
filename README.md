# pavel

Command line tool for easily reading and writing messages to Kafka topics written in Go

## Building

pavel is using [dep](https://github.com/golang/dep) to manage its dependencies.

To download dependencies run:

```sh
$ dep ensure
```

And then build with

```sh
$ go build
```

This should result in a `pavel` executable