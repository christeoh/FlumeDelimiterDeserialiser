package com.example.flume.deserialisers;

/**
 * Created by cteoh on 15/02/2016.
 * From: http://xingwu.me/2014/10/04/Implement-a-Flume-Deserializer-Plugin-to-Import-XML-Files/
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

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
public class TextDelimiterDeserialiser implements EventDeserializer {
    private final static String CLOSING_DELIMITER_KEY = "closingDelimiter";
    private final static String CLOSING_DELIMITER_KEY_DEFAULT = "</element>";
    private final static String IGNORE_TEXT_KEY = "ignoreText";
    private final static String IGNORE_TEXT_DEFAULT = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>";

    private final static String DEFAULT_CHARSET = "UTF-8";
    private final Charset charset = Charset.forName(DEFAULT_CHARSET);
    private InputStreamReader r;
    private final ResettableInputStream in;
    private final static Logger logger = LoggerFactory.getLogger(TextDelimiterDeserialiser.class);
    private boolean isOpen;
    private boolean inTopElementScope = false;
    private String closingDelimiter;
    private String ignoreText;

    public TextDelimiterDeserialiser(Context context, ResettableInputStream inputStream) {
        this.closingDelimiter = context.getString(CLOSING_DELIMITER_KEY, CLOSING_DELIMITER_KEY_DEFAULT);
        // TODO: implement ignoreText in readEvent
        this.ignoreText = context.getString(IGNORE_TEXT_KEY, IGNORE_TEXT_DEFAULT);
        this.in = inputStream;
        try {
            r = new InputStreamReader(new FlumeInputStream(inputStream));
            this.isOpen = true;
        } catch (Exception e) {
            logger.error("Failed to create input stream: "+e.getMessage());
            e.printStackTrace();
        }
    }

    public Event readEvent() throws IOException {
        StringBuilder sb = new StringBuilder();
        Stack<Character> buf = new Stack<Character>();
        try {
            int c;
            int readChars = 0;
            int closingDelimLen = closingDelimiter.length();
            // populate the stack we are looking for
            for (int i = closingDelimLen-1;i>0;i--) {
                buf.push(closingDelimiter.charAt(i));
            }
            while ((c = in.readChar()) != -1) {
                readChars++;

                // FIXME: support \r\n
                if (c == '\n') {
                    continue;
                }

                sb.append((char)c);
                // look for the closing XML tag
                int sbLen = sb.length();
                if (!buf.isEmpty()) {
                    Character bufChar = buf.pop();
                    if (c != bufChar) {
                        // reset entire buffer
                        buf.clear();
                        for (int i = closingDelimLen-1;i>0;i--) {
                            buf.push(closingDelimiter.charAt(i));
                        }
                    }
                }
                if (buf.isEmpty()) {
                    mark();
                    break;
                }
            }

            if (readChars > 0) {
                return EventBuilder.withBody(sb.toString(),charset);
            } else {
                return null;
            }
        } catch (Exception e) {
            System.err.println("Error reading text: "+e.getMessage());
        }
        return null;
    }

    public List<Event> readEvents(int numEvents) throws IOException {
        List<Event> events = new LinkedList<Event>();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
            } else {
                break;
            }
        }
        return events;
    }

    public void mark() throws IOException {
        ensureOpen();
        this.in.mark();
    }

    public void reset() throws IOException {
        ensureOpen();
        this.in.reset();
    }

    public void close() throws IOException {
        ensureOpen();
        this.in.close();
        this.isOpen = false;
    }

    // throws exception if stream is closed
    private void ensureOpen() {
        if (!this.isOpen) {
            throw new IllegalStateException("Stream is closed.");
        }
    }
}
