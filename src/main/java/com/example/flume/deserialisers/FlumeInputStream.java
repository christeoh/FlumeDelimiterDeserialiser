/**
 * Created by cteoh on 16/02/2016.
 * From: http://xingwu.me/2014/10/04/Implement-a-Flume-Deserializer-Plugin-to-Import-XML-Files/
 */
package com.example.flume.deserialisers;

import java.io.IOException;
import java.io.InputStream;

import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeInputStream extends InputStream {
    private static final Logger logger = LoggerFactory.getLogger(FlumeInputStream.class);

    private final ResettableInputStream in;
    public FlumeInputStream(ResettableInputStream input) {
        this.in = input;
    }

    @Override
    public int read() throws IOException {
        try {
            return this.in.read();
        }
        catch (Exception e) {
            logger.error(e.getMessage() + ": "+e.getStackTrace().toString());
            return 0;
        }
    }
}
