# pavel

Command line tool for easily consuming and producing messages to Kafka topics written in Go

## Usage

Detailed usage can be shown with the command

```sh
$ pavel help
```

### Consuming messages

The following command will consume all messages from broker `localhost:9092` and topic `hello` and save these in `hello.log`, once all messages have been consumed the program will exit.

```sh
$ pavel consume localhost:9092 hello hello.log
```

By using `--listen` you can keep listening for new messages.

Starting offset can be manipulated using `--offset`. Default offset is "beginning", other valid options are "earliest", "end", "latest", "unset", "invalid", "stored".

By omitting filename pavel will output to `stdout` instead

```sh
$ pavel consume --listen --offset="latest" localhost:9092 hello
```

### Producing messages

The following command will iterate `hello.log` line by line and produce messages into broker `localhost:9092` and topic `world`

```sh
$ pavel produce localhost:9092 world hello.log
```

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