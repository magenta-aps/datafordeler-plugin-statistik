package dk.magenta.datafordeler.statistik.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Stream;

public class FormatPersonUtils {


    public void csvFormatterAndWriter(Session primary_session,
                          Session secondary_session,
                          List<String> keys,
                          HttpServletRequest request,
                          HttpServletResponse response,
                          CsvMapper csvMapper
                          ) throws IOException {


        System.out.println("Here");

        try{
            System.out.println("inside Here");

            PersonQuery personQuery = new PersonQuery();

            OffsetDateTime now = OffsetDateTime.now();
            personQuery.setRegistrationFrom(now);
            personQuery.setRegistrationTo(now);
            personQuery.setEffectFrom(now);
            personQuery.setEffectTo(now);

            personQuery.applyFilters(primary_session);




            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            Iterator<Map<String, Object>> dataIter = personEntities.map(personEntity -> {
                return formatPerson(personEntity, secondary_session);
            }).iterator();

            CsvSchema.Builder builder = new CsvSchema.Builder();




               /* List<String> keys = Arrays.asList(new String[]{
                            "pnr", "birth_year", "effective_pnr", "status_code",
                            "mother_pnr", "mother_status", "mother_municipality_code", "mother_road_code", "mother_house_number", "mother_door_number", "mother_bnr",
                            "father_pnr", "father_status", "father_municipality_code", "father_road_code", "father_house_number", "father_door_number", "father_bnr"
                            });
                */
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


            SequenceWriter writer = csvMapper.writer(schema).writeValues(response.getOutputStream());

            while (dataIter.hasNext()) {
                System.out.println("Writing");
                writer.write(dataIter.next());
            }
        }finally {
            primary_session.close();
            secondary_session.close();
        }
    }







    public Map<String, Object> formatPerson(PersonEntity person, Session session){

        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put("pnr", person.getPersonnummer());

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffects())
                for (PersonBaseData data : effect.getDataItems()) {

                    PersonNameData firstNameData = data.getName();
                    if(firstNameData != null){
                        item.put("first_name", firstNameData.getFirstNames());
                    }

                    PersonNameData lastNameData = data.getName();
                    if(lastNameData != null){
                        item.put("last_name", lastNameData.getLastName());
                    }



                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null && birthData.getBirthDatetime() != null) {
                        item.put("birth_year", birthData.getBirthDatetime().getYear());

                    }


                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put("status_code", statusData.getStatus());
                    }




                   /* PersonCoreData coreData = data.getCoreData();
                    if (coreData != null) {
                        item.put("effective_pnr", coreData.getCprNumber());
                    }*/





                    //This part of the code is duplicated in the function formatParentPerson.
                    // Check it out how it can be generalized.
                  PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        //item.put("post_code", addressData.getPostalCode());

                        item.put("municipality_code", addressData.getMunicipalityCode());
                        //Locatility need to be here
                        item.put("road_code", addressData.getRoadCode());
                        item.put("house_number", addressData.getHouseNumber());
                        item.put("door_number", addressData.getDoor());
                        item.put("bnr", addressData.getBuildingNumber());


                    }








                    //Missing prod date (not sure about the meaning)


                    //Intended to full fill the own information in contrary to parents
                    /*PersonCoreData personData = data.getCoreData();
                    if (personData != null) {

                        PersonEntity own = QueryManager.getEntity(session, PersonEntity.generateUUID(personData.getCprNumber()), PersonEntity.class);
                        if (own != null) {
                            item.putAll(this.formatParentPerson(own, session, ""));
                        }
                    }
                    */




                    PersonParentData personMotherData = data.getMother();
                    if (personMotherData != null) {
                        item.put("mother_pnr", personMotherData.getCprNumber());
                        PersonEntity mother = QueryManager.getEntity(session, PersonEntity.generateUUID(personMotherData.getCprNumber()), PersonEntity.class);
                        if (mother != null) {
                            item.putAll(this.formatParentPerson(mother, session, "mother_"));
                        }
                    }

                    PersonParentData personFatherData = data.getFather();
                    if (personFatherData != null) {
                        item.put("father_pnr", personFatherData.getCprNumber());
                        PersonEntity father = QueryManager.getEntity(session, PersonEntity.generateUUID(personFatherData.getCprNumber()), PersonEntity.class);
                        if (father != null) {
                            item.putAll(this.formatParentPerson(father, session, "father_"));
                        }
                    }

                    PersonCivilStatusData personSpouseData = data.getCivilStatus();
                    if (personSpouseData != null) {
                        // "civil_status_date"?


                        item.put("spouse_pnr", personSpouseData.getSpouseCpr());
                        PersonEntity spouse = QueryManager.getEntity(session, PersonEntity.generateUUID(personSpouseData.getSpouseCpr()), PersonEntity.class);
                        if (spouse != null) {
                            item.putAll(this.formatParentPerson(spouse, session, "spouse_"));
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
                        //Missing birth_authority code
                        //"moving_in_date"?

                        // "church"?

                        item.put(prefix + "municipality_code", addressData.getMunicipalityCode() );
                        //Locatility need to be here
                        item.put(prefix + "road_code", addressData.getRoadCode());
                        item.put(prefix + "house_number", addressData.getHouseNumber());
                        item.put(prefix + "door_number", addressData.getDoor());
                        item.put(prefix + "bnr", addressData.getBuildingNumber());
                    }






                }

            }
        }
        return item;

    }
}
