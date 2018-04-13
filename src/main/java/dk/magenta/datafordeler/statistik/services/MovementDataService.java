package dk.magenta.datafordeler.statistik.services;
/*ract the following for a person:
    pnr
    birth year
    effective pnr
    status code
    birth municipality code (data missing, import handled in another ticket)
    mother's pnr
    father's pnr
    spouse's pnr
    movement date
    prod date (to be investigated)
    origin municipality code
    origin locality name
    origin road code
    origin house number
    origin floor
    origin door/apartment no.
    origin bnr
    destination municipality code
    destination locality name
    destination road code
    destination house number
    destination floor
    destination door/apartment no.
    destination bnr

Input parameters:
    movement date*/
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.statistik.utils.FormatPersonUtils;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/*Created by Efrin 06-04-2018*/


@RestController
@RequestMapping("/statistik/movement_data")
public class MovementDataService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private DafoUserManager dafoUserManager;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private CprPlugin cprPlugin;

    private FormatPersonUtils personUtils;

    private Logger log = LoggerFactory.getLogger(DeathDataService.class);


    //This function should have the following inputs:
    //movement date

    @RequestMapping(method = RequestMethod.GET, path = "/{cprNummer}", produces = {MediaType.TEXT_PLAIN_VALUE})
    public void getDeath(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException {


        final Session primary_session = sessionManager.getSessionFactory().openSession();
        final Session secondary_session = sessionManager.getSessionFactory().openSession();



        try {

            PersonQuery personQuery = new PersonQuery();

            OffsetDateTime now = OffsetDateTime.now();
            personQuery.setRegistrationFrom(now);
            personQuery.setRegistrationTo(now);
            personQuery.setEffectFrom(now);
            personQuery.setEffectTo(now);

            personQuery.applyFilters(primary_session);
            personUtils = new FormatPersonUtils();

            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            Iterator<Map<String, Object>> dataIter = personEntities.map(personEntity -> {
                return personUtils.formatPerson(personEntity, secondary_session);
            }).iterator();

            CsvSchema.Builder builder = new CsvSchema.Builder();

            List<String> keys = Arrays.asList(new String[]{
                    "pnr", "birth_year", "effective_pnr", "status_code", "birth_municipality_code",



                    "mother_pnr", "father_pnr", "spouse_pnr", "prod_date",

                    "origin_municipality_code", "origin_locality_name", "origin_road_code", "origin_house_number", "origin_floor", "origin_door_number", "origin_bnr",
                    "destination_municipality_code", "destination_locality_name", "destination_road_code", "destination_house_number", "destination_floor", "destination_door_number", "destination_bnr",


            });



                    System.out.println("Keys: --->"+keys.toString());

            for (int i = 0; i < keys.size(); i++) {
                builder.addColumn(new CsvSchema.Column(
                        i, keys.get(i),
                        CsvSchema.ColumnType.NUMBER_OR_STRING
                ));
            }
            CsvSchema schema = builder.build().withHeader();
            response.setContentType("text/csv");


            SequenceWriter writer = csvMapper.writer(schema).writeValues(response.getOutputStream());

            while (dataIter.hasNext()) {
                writer.write(dataIter.next());
            }


        }finally {
            primary_session.close();
            secondary_session.close();
        }


    }
}