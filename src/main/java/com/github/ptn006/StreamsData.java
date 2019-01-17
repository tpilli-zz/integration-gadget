package com.github.ptn006;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamsData {

    public static void streams() {

    try {
        ConfigData.select(InputData.app_id, InputData.dev_stage);

        KStreamBuilder builder = new KStreamBuilder();

        //KStream<String, String> topic = builder.stream("SvcStreams");
        KStream<String, String> topic = builder.<String, String>stream(ConfigData.avroList.get(0));

        KTable<String, Long> counter = topic
                .mapValues(textLine -> textLine.toLowerCase())
                //.filter((dummy, value) -> value.getIsNew())
                .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count("Counts");

        counter.to(Serdes.String(), Serdes.Long(), "svcStreams");

        KafkaStreams streams = new KafkaStreams(builder, KafkaProps.streamsProperties());
        streams.start();
        System.out.println("Streams App is running..");

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    catch(Exception e){

    }

    }
}
