package com.example.flume.deserialisers;

/**
 * Created by cteoh on 15/02/2016.
 * From: http://xingwu.me/2014/10/04/Implement-a-Flume-Deserializer-Plugin-to-Import-XML-Files/
 */

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
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
import org.apache.flume.serialization.ResettableInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class XmlDeserialiser implements EventDeserializer {
    private final static String TOP_LEVEL_ELEMENT = "element";
    private final static String DEFAULT_CHARSET = "UTF-8";
    private final Charset charset = Charset.forName(DEFAULT_CHARSET);
    private XMLStreamReader r;
    private final ResettableInputStream in;
    private final static Logger logger = LoggerFactory.getLogger(XmlDeserialiser.class);
    private boolean isOpen;
    private boolean inTopElementScope = false;

    public XmlDeserialiser(Context context, ResettableInputStream inputStream) {
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        this.in = inputStream;
        try {
            r = inputFactory.createXMLStreamReader(new FlumeInputStream(in));
            this.isOpen = true;
        } catch (XMLStreamException e) {
            logger.error("Failed to create input stream: "+e.getMessage());
            e.printStackTrace();
        }
    }

    public Event readEvent() throws IOException {
        logger.error("Reading a single event from XML stream unsupported. Use readEvents()");
        return null;
    }

    public List<Event> readEvents(int numEvents) throws IOException {
        checkStreamIsOpen();
        List<Event> events = new LinkedList<Event>();
        StringBuilder sb = new StringBuilder();
        try {
            int event = r.getEventType();
            while (true) {

                switch(event) {
                    case XMLStreamConstants.START_DOCUMENT:
                        break;
                    case XMLStreamConstants.END_DOCUMENT:
                        break;
                    case XMLStreamConstants.CHARACTERS:
                        break;
                    case XMLStreamConstants.START_ELEMENT:
                        String name = r.getName().getLocalPart();
                        if (name.equals(TOP_LEVEL_ELEMENT)) {
                            this.inTopElementScope = true;
                            // reset StringBuilder
                            sb = new StringBuilder();
                        }

                        sb.append(String.format("<%s>%s</%s>",name,r.getElementText(),name));
                        Event eventObj = EventBuilder.withBody(sb.toString(),this.charset);
                        events.add(eventObj);
                        //sb.append(r.getElementText());
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        String endName = r.getName().getLocalPart();
                        if (endName.equals(TOP_LEVEL_ELEMENT)) {
                            inTopElementScope = false;
                            sb.append(String.format("</%s>",endName));
                            // end of event reached - build event and return
                            // Build event here.
                            // Optionally add headers with eventObj.setHeaders(Map<String,String>))
                            //Event eventObj = EventBuilder.withBody(sb.toString(),this.charset);
                            //events.add(eventObj);
                        }
                        break;
                }

                if (!r.hasNext()) {
                    break;
                }
                event = r.next();

                if (events.size() >= numEvents) {
                    break;
                }
            }

            return events;
        } catch (XMLStreamException e) {
            logger.error(e.getMessage());
        }
        return events;
    }

    public void mark() throws IOException {
        checkStreamIsOpen();
        this.in.mark();
    }

    public void reset() throws IOException {
        checkStreamIsOpen();
        this.in.reset();
    }

    public void close() throws IOException {
        checkStreamIsOpen();
        this.in.close();
        this.isOpen = false;
    }

    // throws exception if stream is closed
    private void checkStreamIsOpen() {
        if (!this.isOpen) {
            throw new IllegalStateException("Stream is closed.");
        }
    }

}
