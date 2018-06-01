package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBirthData;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static dk.magenta.datafordeler.statistik.services.StatisticsService.cprDataOffset;

public class PersonBirthQuery extends PersonStatisticsQuery {

    public PersonBirthQuery(HttpServletRequest request) {
        super(request);
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        FieldDefinition fieldDefinition = new FieldDefinition(
                PersonBaseData.DB_FIELD_BIRTH,
                null,
                Integer.class,
                LookupDefinition.Operator.NE
        );

        this.applyRegistrationTimes(fieldDefinition);
        this.applyEffectTimes(fieldDefinition);

        lookupDefinition.put(fieldDefinition);

        return lookupDefinition;
    }

}
