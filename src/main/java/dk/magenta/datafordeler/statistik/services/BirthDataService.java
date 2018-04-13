package dk.magenta.datafordeler.statistik.services;

/*Birth data service Extract the following for a person:
    own pnr
    own birth year
    own effective pnr
    own birth municipality code (data missing, import handled in another ticket)
    own status code
    own prod date (to be investigated)

    mother's pnr
    mother's birth municipality code (data missing, import handled in another ticket)
    mother's status code
    mother's municipality code
    mother's locality name
    mother's road code
    mother's house number
    mother's door/apartment no.
    mother's bnr

    father's pnr
    father's birth municipality code (data missing, import handled in another ticket)
    father's status code
    father's municipality code
    father's locality name
    father's road code
    father's house number
    father's door/apartment no.
    father's bnr

Input parameters:
    birth year
    registration before date
    */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
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
import java.util.*;
import java.util.stream.Stream;


/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/birth_data")
public class BirthDataService extends StatisticsService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;


    private Logger log = LoggerFactory.getLogger(BirthDataService.class);


    //This function should have the following inputs:
    // birth year
    // registration before date
    @RequestMapping(method = RequestMethod.GET, path = "/{cprNummer}", produces = {MediaType.TEXT_PLAIN_VALUE})
    public void getBirth(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException {

        final Session primary_session = sessionManager.getSessionFactory().openSession();
        final Session secondary_session = sessionManager.getSessionFactory().openSession();

        PersonQuery personQuery = new PersonQuery();
        OffsetDateTime now = OffsetDateTime.now();
        personQuery.setRegistrationFrom(now);
        personQuery.setRegistrationTo(now);
        personQuery.setEffectFrom(now);
        personQuery.setEffectTo(now);
        personQuery.applyFilters(primary_session);
        Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

        this.writeItems(this.formatItems(personEntities, primary_session, secondary_session), response);
    }


    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                "pnr", "birth_year", "effective_pnr", "status_code",
                "mother_pnr", "mother_status", "mother_municipality_code", "mother_road_code", "mother_house_number", "mother_door_number", "mother_bnr",
                "father_pnr", "father_status", "father_municipality_code", "father_road_code", "father_house_number", "father_door_number", "father_bnr",
                "locality_name", "road_code", "house_number", "door_number", "bnr","municipality_code",
        });
    }

    @Override
    protected CsvMapper getCsvMapper() {
        return this.csvMapper;
    }
}
