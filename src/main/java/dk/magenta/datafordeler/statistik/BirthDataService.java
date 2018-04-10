package dk.magenta.datafordeler.statistik;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/birth_data")
public class BirthDataService {

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

    private Logger log = LoggerFactory.getLogger(BirthDataService.class);

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }

    @RequestMapping(method = RequestMethod.GET, path = "/{cprNummer}", produces = {MediaType.TEXT_PLAIN_VALUE})
    public void getBirth(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException {

        final Session primary_session = sessionManager.getSessionFactory().openSession();
        final Session secondary_session = sessionManager.getSessionFactory().openSession();

                try{
                    PersonQuery personQuery = new PersonQuery();

                    OffsetDateTime now = OffsetDateTime.now();
                    personQuery.setRegistrationFrom(now);
                    personQuery.setRegistrationTo(now);
                    personQuery.setEffectFrom(now);
                    personQuery.setEffectTo(now);

                    personQuery.applyFilters(primary_session);
                    //this.applyAreaRestrictionsToQuery(personQuery, user);

                    Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

                        Iterator<Map<String, Object>> dataIter = personEntities.map(personEntity -> {
                            return formatPerson(personEntity, secondary_session);
                        }).iterator();

                        CsvSchema.Builder builder = new CsvSchema.Builder();




                    List<String> keys = Arrays.asList(new String[]{
                                    "pnr", "birth_year", "effective_pnr", "status",
                            "mother_pnr","mother_status", "mother_municipality_code", "mother_road code", "mother_house_number", "mother_door_number", "mother_bnr",
                             "father_pnr","father_status", "father_municipality_code", "father_road code", "father_house_number", "father_door_number", "father_bnr"
                            });
                    System.out.println(keys.toString());

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

                    //throw new HttpNotFoundException("No entity with CPR number " + cprNummer + " was found");
                }finally {
                    primary_session.close();
                    secondary_session.close();
                }
    }


    private Map<String, Object> formatPerson(PersonEntity person, Session session){

        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put("pnr", person.getPersonnummer());

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffects()){
                for (PersonBaseData data: effect.getDataItems()){

                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null && birthData.getBirthDatetime() != null) {
                        item.put("birth_year", birthData.getBirthDatetime().getYear());

                    }

                    PersonCoreData coreData = data.getCoreData();
                    if(coreData != null) {
                        item.put("effective_pnr", coreData.getCprNumber());
                    }

                    //Missing birth_authority code

                    PersonStatusData statusData = data.getStatus();
                    if(statusData != null){
                        item.put("status", statusData.getStatus());
                    }

                    //Missing prod date (not sure about the meaning)

                    PersonParentData personMotherData = data.getMother();
                    if(personMotherData != null){
                      item.put("mother_pnr", personMotherData.getCprNumber());
                            PersonEntity mother = QueryManager.getEntity(session, PersonEntity.generateUUID(personMotherData.getCprNumber()), PersonEntity.class);
                            if(mother != null){
                                item.putAll(this.formatParentPerson(mother, session, "mother_"));
                            }
                    }

                    PersonParentData personFatherData = data.getFather();
                    if(personFatherData != null){
                        item.put("father_pnr", personFatherData.getCprNumber());
                        PersonEntity father = QueryManager.getEntity(session, PersonEntity.generateUUID(personFatherData.getCprNumber()), PersonEntity.class);
                        if(father != null){
                            item.putAll(this.formatParentPerson(father, session, "father_"));
                        }
                    }

                }

            }
        }
        return item;

    }

    private Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix){

        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put(prefix + "pnr", person.getPersonnummer());

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffects()){
                for (PersonBaseData data: effect.getDataItems()){





                    PersonStatusData statusData = data.getStatus();
                    if(statusData != null){
                        item.put(prefix + "status", statusData.getStatus());
                    }

                    PersonAddressData addressData = data.getAddress();
                    if(addressData != null){
                        item.put(prefix + "municipality_code", addressData.getMunicipalityCode() );
                        //Locatility need to be here
                        item.put(prefix + "road code", addressData.getRoadCode());
                        item.put(prefix + "house_number", addressData.getHouseNumber());
                        item.put(prefix + "door_number", addressData.getDoor());
                        item.put(prefix + "bnr", addressData.getBuildingNumber());
                    }






                }

            }
        }
        return item;

    }

//    @SpringBootApplication
//    public static class Application {
//
//        public static void main(String[] args) {
//            SpringApplication.run(Application.class, args);
//
//            System.out.println("Hi there");
//        }
//    }
}
