package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBirthData;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public class PersonBirthQuery extends PersonQuery {

    private LocalDateTime birthDateTimeBefore = null;

    public void setBirthDateTimeBefore(LocalDateTime birthDateTimeBefore) {
        this.birthDateTimeBefore = birthDateTimeBefore;
    }


    private LocalDateTime birthDateTimeAfter = null;

    public void setBirthDateTimeAfter(LocalDateTime birthDateTimeAfter) {
        this.birthDateTimeAfter = birthDateTimeAfter;
    }


    private OffsetDateTime registrationTimeAfter = null;

    public void setRegistrationTimeAfter(OffsetDateTime registrationTimeAfter) {
        this.registrationTimeAfter = registrationTimeAfter;
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);
        FieldDefinition fieldDefinition = null;
        if (this.birthDateTimeAfter != null) {
            fieldDefinition = lookupDefinition.put(
                    PersonBaseData.DB_FIELD_BIRTH + LookupDefinition.separator + PersonBirthData.DB_FIELD_BIRTH_DATETIME,
                    this.birthDateTimeAfter,
                    LocalDateTime.class,
                    LookupDefinition.Operator.GTE
            );
        }

        if (this.birthDateTimeBefore != null) {
            if (fieldDefinition != null) {
                fieldDefinition.and(
                        PersonBaseData.DB_FIELD_BIRTH + LookupDefinition.separator + PersonBirthData.DB_FIELD_BIRTH_DATETIME,
                        this.birthDateTimeBefore,
                        LocalDateTime.class,
                        LookupDefinition.Operator.LTE
                );
            } else {
                lookupDefinition.put(
                        PersonBaseData.DB_FIELD_BIRTH + LookupDefinition.separator + PersonBirthData.DB_FIELD_BIRTH_DATETIME,
                        this.birthDateTimeBefore,
                        LocalDateTime.class,
                        LookupDefinition.Operator.LTE
                );
            }
        }

        if (this.registrationTimeAfter != null) {
            FieldDefinition registrationDefinition = new FieldDefinition(
                    LookupDefinition.registrationref + LookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                    this.registrationTimeAfter,
                    OffsetDateTime.class,
                    LookupDefinition.Operator.LT
            );
            registrationDefinition.and(
                    PersonBaseData.DB_FIELD_BIRTH + LookupDefinition.separator + PersonBirthData.DB_FIELD_BIRTH_PLACE_CODE,
                    null,
                    Integer.class,
                    LookupDefinition.Operator.NE
            );
            registrationDefinition.invert();
            lookupDefinition.put(registrationDefinition);
        }

        return lookupDefinition;
    }

}
