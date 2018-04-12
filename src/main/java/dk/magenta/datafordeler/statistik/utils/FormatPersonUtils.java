package dk.magenta.datafordeler.statistik.utils;

import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import org.hibernate.Session;

import java.util.HashMap;
import java.util.Map;

public class FormatPersonUtils {

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


                   /* PersonCoreData coreData = data.getCoreData();
                    if (coreData != null) {
                        item.put("effective_pnr", coreData.getCprNumber());
                    }*/








                    //This part of the code is duplicated in the function formatParentPerson.
                    // Check it out how it can be generalized.
          /*          PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        //item.put("post_code", addressData.getPostalCode());

                        //"moving_in_date"?

                        // "church"?

                        item.put("municipality_code", addressData.getMunicipalityCode());
                        //Locatility need to be here
                        item.put("road_code", addressData.getRoadCode());
                        item.put("house_number", addressData.getHouseNumber());
                        item.put("door_number", addressData.getDoor());
                        item.put("bnr", addressData.getBuildingNumber());
                    }
*/

                    //Missing birth_authority code

                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put("status_code", statusData.getStatus());
                    }

                    //Missing prod date (not sure about the meaning)

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
