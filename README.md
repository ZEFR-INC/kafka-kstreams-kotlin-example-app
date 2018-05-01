This is an example app that reads and writes to kafka using kotlin and spring boot.

To run, start up zookeeper, kafka, and schema registry locally. Then create the topics VideoMessage and ReviewableMessage

TODO: Better explanation here :)

Start the app, standard spring boot style

```
gradle build
java -jar kafka-kstreams-kotlin-example-app.jar
```

Send video messages to the VideoMessage topic. Read reviewable messages from the ReviewableMessage






