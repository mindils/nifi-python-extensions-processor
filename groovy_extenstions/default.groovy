import groovy.json.*;
import org.apache.commons.io.IOUtils;
import java.nio.charset.*;
import java.lang.*;


static def run(payload) {
    return payload
}

def flowFile = session.get();

if (flowFile == null) { return; }


flowFile = session.write(flowFile, { inputStream, outputStream ->
          def data = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
          def input = new JsonSlurper().parseText( data );
          def result = run(input);
          String finalJson = JsonOutput.toJson result;
          outputStream.write(finalJson.getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

session.transfer(flowFile, REL_SUCCESS)