package com.example.flume.deserialisers;

/**
 * Created by cteoh on 15/02/2016.
 */

import java.io.IOException;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class XmlDeserialiser implements EventDeserializer {

    XMLStreamReader r;

    public XmlDeserialiser() {
        // TODO
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();

        try {
            r = inputFactory.createXMLStreamReader()
        }
    }

    @Override
    public Event readEvent() throws IOException {
        return null;
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        return null;
    }

    @Override
    public void mark() throws IOException {

    }

    @Override
    public void reset() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

}
