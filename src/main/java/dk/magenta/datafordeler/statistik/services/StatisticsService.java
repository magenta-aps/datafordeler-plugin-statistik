package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.exception.MissingParameterException;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.hibernate.Session;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public abstract class StatisticsService {

    protected abstract List<String> getColumnNames();

    protected abstract CsvMapper getCsvMapper();

    protected abstract Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter);

    protected abstract Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix);

    public static final String INCLUSION_DATE_PARAMETER = "inclusionDate";
    public static final String BEFORE_DATE_PARAMETER = "beforeDate";
    public static final String AFTER_DATE_PARAMETER = "afterDate";

    public static final String EFFECT_DATE_PARAMETER = "effectDate";


    // Each service may need their own implementation of this, in which case they are welcome to override it
    protected PersonQuery getQuery(HttpServletRequest request) {
        OffsetDateTime livingInGreenlandAtDate = Query.parseDateTime(request.getParameter(INCLUSION_DATE_PARAMETER));
        PersonQuery personQuery = new PersonQuery();
        if (livingInGreenlandAtDate != null) {
            personQuery.setEffectFrom(livingInGreenlandAtDate);
            personQuery.setEffectTo(livingInGreenlandAtDate);
        }
        return personQuery;
    }

    protected void writeItems(Iterator<Map<String, Object>> items, HttpServletResponse response) throws IOException {
        System.out.println("items: "+items);
        CsvSchema.Builder builder = new CsvSchema.Builder();
        List<String> keys = this.getColumnNames();
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














    public Iterator<Map<String, Object>> formatItems(Stream<PersonEntity> personEntities, Session secondary_session, Filter filter) {
        return personEntities.map(personEntity -> formatPerson(personEntity, secondary_session, filter)).iterator();
    }



    /*
    TODO: Find a way to format a person differently for each service (ie. each service wants different fields set)
    Populating the map with all fields and then filtering in the service will introduce unnecessary overhead because
    we're fetching data we don't need.
    Perhaps use a Set of keys we're interested in, from getColumnNames()?
     */
  /*  public Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter){

        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put("pnr", person.getPersonnummer());




        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {



                    //Check the type of service here and define with constructor to use for that service.
                    //There most be an integer or any other kind of flag for the service.
                     //   it can be a simple if checking of an integer

                    /*
                    PersonNameData firstNameData = data.getName();
                    if (firstNameData != null) {
                        item.put("first_name", firstNameData.getFirstNames());
                    }

                    PersonNameData lastNameData = data.getName();
                    if (lastNameData != null) {
                        item.put("last_name", lastNameData.getLastName());
                    }*/

        /*            PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthDatetime() != null) {
                            item.put("birth_year", birthData.getBirthDatetime().getYear());
                        }
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put("birth_authority", birthData.getBirthPlaceCode());
                        }
                    }


                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put("status_code", statusData.getStatus());
                    }




                   /* PersonCoreData coreData = data.getCoreData();
                    if (coreData != null) {
                        item.put("effective_pnr", coreData.getCprNumber());
                    }*/

     /*               item.put("effective_pnr", person.getPersonnummer());
                    PersonCoreData coreData = data.getCoreData();
                    if (coreData != null) {
                        item.put("effective_pnr", coreData.getCprNumber());
                    }


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


     /*               PersonParentData personMotherData = data.getMother();
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
/*
                    PersonCivilStatusData personSpouseData = data.getCivilStatus();
                    if (personSpouseData != null) {
                        // "civil_status_date"?


                        item.put("spouse_pnr", personSpouseData.getSpouseCpr());
                        PersonEntity spouse = QueryManager.getEntity(session, PersonEntity.generateUUID(personSpouseData.getSpouseCpr()), PersonEntity.class);
                        if (spouse != null) {
                            item.putAll(this.formatParentPerson(spouse, session, "spouse_"));
                        }
                    }
*/
 /*               }
            }
        }
        return item;

    }*/

    protected void requireParameter(String parameterName, String parameterValue) throws MissingParameterException {
        if (parameterValue == null) {
            throw new MissingParameterException(parameterName);
        }
    }
}

