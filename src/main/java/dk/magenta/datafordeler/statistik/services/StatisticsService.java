package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.Lookup;
import dk.magenta.datafordeler.statistik.utils.LookupService;
import org.hibernate.Session;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public abstract class StatisticsService {

    protected abstract List<String> getColumnNames();

    protected abstract CsvMapper getCsvMapper();

    public static final String INCLUSION_DATE_PARAMETER = "inclusionDate";
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

    public Iterator<Map<String, Object>> formatItems(Stream<PersonEntity> personEntities, Session secondary_session, Filter filter) {
        return personEntities.map(personEntity -> formatPerson(personEntity, secondary_session, filter)).iterator();
    }



    public Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter){

        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put("pnr", person.getPersonnummer());

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {
/*
                    PersonNameData firstNameData = data.getName();
                    if (firstNameData != null) {
                        item.put("first_name", firstNameData.getFirstNames());
                    }

                    PersonNameData lastNameData = data.getName();
                    if (lastNameData != null) {
                        item.put("last_name", lastNameData.getLastName());
                    }*/

                    PersonBirthData birthData = data.getBirth();
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

                    item.put("effective_pnr", person.getPersonnummer());
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
                }
            }
        }
        return item;
    }


    private Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix){

        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put(prefix + "pnr", person.getPersonnummer());
        LookupService lookupService = new LookupService(session);

        for (PersonRegistration registration: person.getRegistrations()) {
            for (PersonEffect effect: registration.getEffects()) {
                for (PersonBaseData data: effect.getDataItems()) {
                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null){
                        item.put(prefix + "status", statusData.getStatus());
                    }

                    PersonAddressData addressData = data.getAddress();
                    if(addressData != null){
                        Lookup lookup = lookupService.doLookup(addressData.getMunicipalityCode(), addressData.getRoadCode());

                        item.put(prefix + "municipality_code", addressData.getMunicipalityCode() );
                        item.put(prefix + "road_code", addressData.getRoadCode());
                        item.put(prefix + "house_number", addressData.getHouseNumber());
                        item.put(prefix + "door_number", addressData.getDoor());
                        item.put(prefix + "bnr", addressData.getBuildingNumber());

                        if (lookup.localityName != null) {
                            item.put(prefix + "locality", lookup.localityName);
                        }
                    }


                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put(prefix + "birth_authority", birthData.getBirthPlaceCode());
                        }
                    }



                }
            }
        }
        return item;
    }


    protected void writeItems(Iterator<Map<String, Object>> items, HttpServletResponse response) throws IOException {
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
}
