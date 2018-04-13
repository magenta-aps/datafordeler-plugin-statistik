package dk.magenta.datafordeler.statistik.services;
/*Extract the following for a person:
    pnr
    birth year
    firstname
    lastname
    status code
    birth municipality code (data missing, import handled in another ticket)
    mother's pnr
    father's pnr
    civil status
    spouse's pnr
    municipality code
    locality name
    road code
    house number
    floor
    door/apartment no.
    bnr
    moving-in date
    postcode
    civil status date
    church (to be investigated)

Input parameters:
    living in Greenland on date*/

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;


/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/status_data")
public class StatusDataService extends StatisticsService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    private Logger log = LoggerFactory.getLogger(BirthDataService.class);


    //This function should have the following inputs:
    //living in Greenland on date


    public static int[] glMunicipalityCodes = new int[]{955, 956, 957, 958, 961};

    @RequestMapping(method = RequestMethod.GET, path = "/{cprNummer}", produces = {MediaType.TEXT_PLAIN_VALUE})
    public void getBirth(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException {

        OffsetDateTime livingInGreenlandAtDate = Query.parseDateTime(request.getParameter(INCLUSION_DATE_PARAMETER));
        OffsetDateTime effectDate = Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER));
        Filter filter = new Filter(effectDate);

        final Session primary_session = sessionManager.getSessionFactory().openSession();
        final Session secondary_session = sessionManager.getSessionFactory().openSession();

        try {
            PersonQuery personQuery = new PersonQuery();
            personQuery.setEffectFrom(livingInGreenlandAtDate);
            personQuery.setEffectTo(livingInGreenlandAtDate);
            personQuery.applyFilters(primary_session);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            this.writeItems(this.formatItems(personEntities, primary_session, secondary_session, filter), response);
        } finally {
            primary_session.close();
            secondary_session.close();
        }
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                "pnr", "birth_year", "first_name", "last_name", "status_code",
                "birth_municipality", "mother_pnr","father_pnr", "spouse_pnr", "civil_status",
                "municipality_code", "locality_name", "road_code", "house_number", "door_number",
                "bnr", "moving_in_date", "post_code", "civil_status_date", "church"

        });
    }

    @Override
    protected CsvMapper getCsvMapper() {
        return this.csvMapper;
    }
}
