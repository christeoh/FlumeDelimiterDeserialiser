# Text Delimiter Deserialiser for Flume
A Flume Deserialiser that can produce events based on a specific line delimiter. It also ignores text when configured to do so.

Has the following configuration parameters: 
* closingDelimiter - what text to split on.
* ignoreText - what text to ignore from stream.

##Build instructions
Build the jar file using "mvn package" command from the top level folder.

##Deployment instructions
Copy the jar file (TextDelimiterDeserialiser-1.0.jar) in "target" folder to /usr/jars and symlink to /usr/lib/flume-ng/lib

Sample Flume configuration
<pre>
tier1.sources.r1.type = spooldir
tier1.sources.r1.spoolDir = /tmp/flume/source/
tier1.sources.r1.fileSuffix = .COMPLETED
tier1.sources.r1.deletePolicy = immediate
tier1.sources.r1.deserializer = com.example.flume.deserialisers.TextDelimiterDeserialiser$Builder
tier1.sources.r1.deserializer.closingDelimiter = "</element>"
tier1.sources.r1.deserializer.ignoreText = <?xml version="1.0"?>
</pre>
