package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class StatisticsService {

    protected abstract List<String> getColumnNames();

    protected abstract CsvMapper getCsvMapper();

    protected void writeItems(Iterator<Map<String, Object>> items, HttpServletResponse response) throws IOException {

        CsvSchema.Builder builder = new CsvSchema.Builder();

        List<String> keys = this.getColumnNames();

        System.out.println(keys.toString());

        for (int i = 0; i < keys.size(); i++) {
            builder.addColumn(new CsvSchema.Column(
                    i, keys.get(i),
                    CsvSchema.ColumnType.NUMBER_OR_STRING
            ));
        }
        CsvSchema schema = builder.build().withHeader();
        response.setContentType("text/csv");
        System.out.println("Response in class: " + response.toString());

        SequenceWriter writer = this.getCsvMapper().writer(schema).writeValues(response.getOutputStream());

        while (items.hasNext()) {
            writer.write(items.next());
        }
    }
}
