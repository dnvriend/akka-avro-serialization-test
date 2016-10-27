# akka-avro-serialization-test
A small study project how to serialize with apache Avro and akka-persistence using a single serializer

## How to use
- launch sbt
- avro:generate

## Resources
- [Martin Kleppmann - Schema evolution in Avro, Protocol Buffers and Thrift](https://martin.kleppmann.com/2012/12/05/schema-evolution-in-avro-protocol-buffers-thrift.html)
- [Stephen Samuel - Scala4s](https://github.com/sksamuel/avro4s)
- [SBT - sbt-avro](https://github.com/sbt/sbt-avro)
- [Julian Peeters - Avrohugger](https://github.com/julianpeeters/avrohugger)
- [Alexis Seigneurin - Kafka, Spark and Avro - Part 3, Producing and consuming Avro messages](http://aseigneurin.github.io/2016/03/04/kafka-spark-avro-producing-and-consuming-avro-messages.html)

## Stackoverflow
- [Stackoverflow - Does Avro schema evolution require access to both old and new schemas? TL;DR: yes it does](http://stackoverflow.com/questions/12165589/does-avro-schema-evolution-require-access-to-both-old-and-new-schemas)
- [Stackoverflow - Reading Avro file gives AvroTypeException: missing required field error (even though the new field is declared null in schema)](http://stackoverflow.com/questions/38775322/reading-avro-file-gives-avrotypeexception-missing-required-field-error-even-th)
- [Stackoverflow - Is this the proper way to initialize null references in Scala? TL;DR: def foo[A >: Null] = { a: A = null }](http://stackoverflow.com/questions/2440134/is-this-the-proper-way-to-initialize-null-references-in-scala)

## Avro
- [Apache Avro - Getting started](http://avro.apache.org/docs/1.8.1/gettingstartedjava.html)
- [Apache Avro - Specification](http://avro.apache.org/docs/current/spec.html)
- [Apache Avro - Parsing schemas](http://avro.apache.org/docs/1.7.2/api/java/org/apache/avro/io/parsing/doc-files/parsing.html)
- [Apache Avro - service for holding schemas](https://issues.apache.org/jira/browse/AVRO-1124)