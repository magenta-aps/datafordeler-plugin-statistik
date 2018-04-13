package dk.magenta.datafordeler.statistik.services;
/*Extract the following for a person:
    status code
    death date
    prod date (to be investigated)
    pnr
    birth year

    mother's pnr
    father's pnr
    spouse's pnr

    effective pnr
    status code
    birth municipality code (data missing, import handled in another ticket)
    municipality code
    locality name
    road code
    house number
    door/apartment no.
    bnr

Input parameters:
    death year
    registration before date
    registration after date
*/
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
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
import java.util.Arrays;
import java.util.List;

/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/death_data")
public class DeathDataService {
    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private CprPlugin cprPlugin;

    private Logger log = LoggerFactory.getLogger(DeathDataService.class);


    //This function should have the following inputs:
    // death year
    //    registration before date
    //    registration after date
    @RequestMapping(method = RequestMethod.GET, path = "/{cprNummer}", produces = {MediaType.TEXT_PLAIN_VALUE})
    public void getDeath(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException {

        final Session primary_session = sessionManager.getSessionFactory().openSession();
        final Session secondary_session = sessionManager.getSessionFactory().openSession();


        List<String> keys = Arrays.asList(new String[]{
                "status_code", "death_date", "prod_date", "pnr", "birth_year",

                "mother_pnr", "father_pnr", "spouse_pnr", "effective_pnr",

                "status_code","birth_municipality", "municipality_code",
                "locality_name", "road_code", "house_number", "door_number", "bnr"


        });

        FormatPersonUtils personUtils = new FormatPersonUtils();

        personUtils.csvFormatterAndWriter(
                primary_session,
                secondary_session,
                keys,
                request,
                response,
                csvMapper);


    }




}