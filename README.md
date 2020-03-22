# Kafka Streams Apps
This repository provides an environment to experiment with and test
their Kafka Streams applications without having to worry about
standing up and managing Kafka and Zookeeper (e.g. resetting their
states between runs). It does this by spinning up an embedded Kafka
and an embedded Zookeeper just for the duration of the test.

There is provided example to demonstrate how one can write their own
Kafka Streams application (SimpleStream) and subsequently test it
(SimpleStreamTest). This example application simply prints the
contents of the input stream and thus the test consists of eye-
balling the printed output. However, in addition to the produce
method used for writing data into topics, EmbeddedKafka also
provides a consume method to read data from topics that can used to
automate tests. An example of writing and reading data can be found
in the main function of EmbeddedKafka.
