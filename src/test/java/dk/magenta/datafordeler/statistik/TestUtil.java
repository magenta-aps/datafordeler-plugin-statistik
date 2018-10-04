package dk.magenta.datafordeler.statistik;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TestUtil {

    @Autowired
    private ObjectMapper objectMapper;

    public ArrayNode csvToJson(String csv) {
        ArrayNode arrayNode = objectMapper.createArrayNode();
        String[] lines = csv.split("\n");
        String[] headers = lines[0].split(";");
        for (int i=1; i<lines.length; i++) {
            ObjectNode objectNode = objectMapper.createObjectNode();
            String[] values = lines[i].split(";");
            for (int j=0; j<values.length; j++) {
                objectNode.put(strip(headers[j]), strip(values[j]));
            }
            arrayNode.add(objectNode);
        }
        return arrayNode;
    }

    public String csvToJsonString(String csv) throws JsonProcessingException {
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(
                this.csvToJson(csv)
        );
    }

    private static String strip(String subject) {
        return subject.replaceAll("^\"|\"$", "");
    }


}
