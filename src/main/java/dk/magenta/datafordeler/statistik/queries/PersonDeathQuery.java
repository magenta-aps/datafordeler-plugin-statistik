package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonStatusData;

import javax.servlet.http.HttpServletRequest;

public class PersonDeathQuery extends PersonStatisticsQuery {

    public PersonDeathQuery(HttpServletRequest request) {
        super(request);
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();

        FieldDefinition fieldDefinition = new FieldDefinition(
                PersonBaseData.DB_FIELD_STATUS + LookupDefinition.separator + PersonStatusData.DB_FIELD_STATUS,
                90,
                Integer.class
        );

        this.applyRegistrationTimes(fieldDefinition);
        this.applyEffectTimes(fieldDefinition);

        lookupDefinition.put(fieldDefinition);

        return lookupDefinition;
    }

}
