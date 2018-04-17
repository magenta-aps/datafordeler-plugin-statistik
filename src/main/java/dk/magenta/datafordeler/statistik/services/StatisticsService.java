package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.hibernate.Session;
import org.springframework.http.HttpStatus;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public abstract class StatisticsService {

    protected void get(HttpServletRequest request, HttpServletResponse response) throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {
        this.requireParameter(EFFECT_DATE_PARAMETER, request.getParameter(EFFECT_DATE_PARAMETER));
        Filter filter = new Filter(Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER)));

        final Session primary_session = this.getSessionManager().getSessionFactory().openSession();
        final Session secondary_session = this.getSessionManager().getSessionFactory().openSession();

        try {
            PersonQuery personQuery = this.getQuery(request);
            personQuery.applyFilters(primary_session);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            int written = this.writeItems(this.formatItems(personEntities, secondary_session, filter), response);
            if (written == 0) {
                response.sendError(HttpStatus.NO_CONTENT.value());
            }
        } finally {
            primary_session.close();
            secondary_session.close();
        }
    }

    protected abstract List<String> getColumnNames();

    protected abstract SessionManager getSessionManager();

    protected abstract CsvMapper getCsvMapper();

    protected abstract Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter);

    public static final String INCLUSION_DATE_PARAMETER = "inclusionDate";
    public static final String BEFORE_DATE_PARAMETER = "beforeDate";
    public static final String AFTER_DATE_PARAMETER = "afterDate";
    public static final String EFFECT_DATE_PARAMETER = "effectDate";


     protected PersonQuery getQuery(HttpServletRequest request) {
        OffsetDateTime livingInGreenlandAtDate = Query.parseDateTime(request.getParameter(INCLUSION_DATE_PARAMETER));
        PersonQuery personQuery = new PersonQuery();
        if (livingInGreenlandAtDate != null) {
            personQuery.setEffectFrom(livingInGreenlandAtDate);
            personQuery.setEffectTo(livingInGreenlandAtDate);
        }
        return personQuery;
    }

    protected int writeItems(Iterator<Map<String, Object>> items, HttpServletResponse response) throws IOException {
        CsvSchema.Builder builder = new CsvSchema.Builder();
        builder.setColumnSeparator(';');
        List<String> keys = this.getColumnNames();
        for (int i = 0; i < keys.size(); i++) {
            builder.addColumn(new CsvSchema.Column(
                    i, keys.get(i),
                    CsvSchema.ColumnType.STRING
            ));
        }
        CsvSchema schema = builder.build().withHeader();
        response.setContentType("text/csv");
        SequenceWriter writer = this.getCsvMapper().writer(schema).writeValues(response.getOutputStream());
        int written;
        for (written = 0; items.hasNext(); written++) {
            writer.write(items.next());
        }
        return written;
    }

    public Iterator<Map<String, Object>> formatItems(Stream<PersonEntity> personEntities, Session secondary_session, Filter filter) {
        return personEntities.map(personEntity -> formatPerson(personEntity, secondary_session, filter)).iterator();
    }

    protected void requireParameter(String parameterName, String parameterValue) throws MissingParameterException {
        if (parameterValue == null) {
            throw new MissingParameterException(parameterName);
        }
    }

    protected static DateTimeFormatter dmyFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
}


