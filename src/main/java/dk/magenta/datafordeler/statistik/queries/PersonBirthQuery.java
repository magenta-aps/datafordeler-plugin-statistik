package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBirthData;

import java.time.LocalDateTime;

public class PersonBirthQuery extends PersonQuery {

    private LocalDateTime birthDateTimeBefore = null;

    public void setBirthDateTimeBefore(LocalDateTime birthDateTimeBefore) {
        this.birthDateTimeBefore = birthDateTimeBefore;
    }



    private LocalDateTime birthDateTimeAfter = null;

    public void setBirthDateTimeAfter(LocalDateTime birthDateTimeAfter) {
        this.birthDateTimeAfter = birthDateTimeAfter;
    }



    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        LookupDefinition.FieldDefinition fieldDefinition = null;
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

        return lookupDefinition;
    }

}
