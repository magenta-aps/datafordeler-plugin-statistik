package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBirthData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonStatusData;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class PersonDeathQuery extends PersonQuery {

    private OffsetDateTime deathDateTimeBefore = null;

    public void setDeathDateTimeBefore(LocalDateTime deathDateTimeBefore) {
        this.deathDateTimeBefore = deathDateTimeBefore.atOffset(ZoneOffset.UTC);
    }



    private OffsetDateTime deathDateTimeAfter = null;

    public void setDeathDateTimeAfter(LocalDateTime deathDateTimeAfter) {
        this.deathDateTimeAfter = deathDateTimeAfter.atOffset(ZoneOffset.UTC);
    }



    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();

        if (this.deathDateTimeAfter != null || this.deathDateTimeBefore != null) {
            LookupDefinition.FieldDefinition fieldDefinition = lookupDefinition.put(PersonBaseData.DB_FIELD_STATUS + LookupDefinition.separator + PersonStatusData.DB_FIELD_STATUS, 90, Integer.class);

            if (this.deathDateTimeAfter != null) {
                fieldDefinition.and(LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM, this.deathDateTimeAfter, OffsetDateTime.class, LookupDefinition.Operator.GTE);
            }

            if (this.deathDateTimeBefore != null) {
                fieldDefinition.and(LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM, this.deathDateTimeBefore, OffsetDateTime.class, LookupDefinition.Operator.LTE);
            }
        }

        return lookupDefinition;
    }

}
