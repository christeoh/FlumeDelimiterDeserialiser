/**
 * Created by cteoh on 16/02/2016.
 */
package com.example.flume.deserialisers;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.flume.deserialisers.XmlDeserialiser;

public class XmlDeserialiserTest {

    private static final boolean CLEANUP = true;
    private static final Logger logger = LoggerFactory.getLogger(XmlDeserialiserTest.class);

    private File file, meta;

    @Before
    public void setup() throws Exception {
        BasicConfigurator.configure();
    }

    @After
    public void tearDown() throws Exception {
        if (CLEANUP) {
            meta.delete();
        }
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Test
    public void testReadEvents() throws IOException {
        file = new File("target/test.xml").getAbsoluteFile();
        logger.info("Data file: {}", file);
        meta = File.createTempFile("test", ".avro");
        logger.info("PositionTracker meta file: {}", meta);
        meta.delete(); // We want the filename but not the empty file

        PositionTracker tracker = DurablePositionTracker.getInstance(meta, file.getPath());
        ResettableInputStream in = new ResettableFileInputStream(file, tracker);
        List<Event> events;

        EventDeserializer des = new XmlDeserialiser(new Context(), in);
        events = des.readEvents(10);
        //System.out.println("Event: " + events.size());
        for (Event e : events) {
            System.out.println("Event: " + new String(e.getBody()));
        }
        des.close();
    }

    @Test
    public void testNestedReadEvents() throws IOException {
        file = new File("target/nestedtest.xml").getAbsoluteFile();
        logger.info("Data file: {}", file);
        meta = File.createTempFile("test", ".avro");
        logger.info("PositionTracker meta file: {}", meta);
        meta.delete(); // We want the filename but not the empty file

        PositionTracker tracker = DurablePositionTracker.getInstance(meta, file.getPath());
        ResettableInputStream in = new ResettableFileInputStream(file, tracker);
        List<Event> events;

        EventDeserializer des = new XmlDeserialiser(new Context(), in);
        events = des.readEvents(1);
        //System.out.println("Event: " + events.size());
        for (Event e : events) {
            System.out.println("Event: " + new String(e.getBody()));
        }
        des.close();
    }

    @Test
    public void testNoNewLineReadEvents() throws IOException {
        file = new File("target/nonewlinetest.xml").getAbsoluteFile();
        logger.info("Data file: {}", file);
        meta = File.createTempFile("test", ".avro");
        logger.info("PositionTracker meta file: {}", meta);
        meta.delete(); // We want the filename but not the empty file

        PositionTracker tracker = DurablePositionTracker.getInstance(meta, file.getPath());
        ResettableInputStream in = new ResettableFileInputStream(file, tracker);
        List<Event> events;

        EventDeserializer des = new XmlDeserialiser(new Context(), in);
        events = des.readEvents(10);
        //System.out.println("Event: " + events.size());
        for (Event e : events) {
            System.out.println("Event: " + new String(e.getBody()));
        }
        des.close();
    }
}
