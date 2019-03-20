package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.BaseLookupDefinition;
import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.records.person.data.CivilStatusDataRecord;
import dk.magenta.datafordeler.statistik.utils.CivilStatusFilter;

import javax.servlet.http.HttpServletRequest;

public class PersonCivilStatusQuery extends PersonStatisticsQuery {

    private String civilStatus;

    public PersonCivilStatusQuery(HttpServletRequest request) {
        super(request);
    }

    public PersonCivilStatusQuery(CivilStatusFilter filter) {
        super(filter);
        civilStatus = filter.getCivilStatus();
    }

    @Override
    public BaseLookupDefinition getLookupDefinition() {
        BaseLookupDefinition lookupDefinition = super.getLookupDefinition();

        FieldDefinition fieldDefinition = null;

        if(civilStatus==null) {
            fieldDefinition = new FieldDefinition(PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS, "G", String.class);
            fieldDefinition.or(new FieldDefinition(PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS, "F", String.class));
            fieldDefinition.or(new FieldDefinition(PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS, "E", String.class));
            fieldDefinition.or(new FieldDefinition(PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS, "P", String.class));
            fieldDefinition.or(new FieldDefinition(PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS, "O", String.class));
            fieldDefinition.or(new FieldDefinition(PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS, "L", String.class));
            fieldDefinition.or(new FieldDefinition(PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS, "D", String.class));
        } else {
            fieldDefinition = new FieldDefinition(PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS, civilStatus, String.class);
        }

        this.applyOriginTimes(fieldDefinition);
        this.applyRegistrationTimes(fieldDefinition);
        this.applyEffectTimes(fieldDefinition);
        lookupDefinition.put(fieldDefinition);
        return lookupDefinition;
    }
}