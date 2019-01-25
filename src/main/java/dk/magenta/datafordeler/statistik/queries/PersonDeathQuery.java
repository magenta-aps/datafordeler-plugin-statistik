package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.BaseLookupDefinition;
import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.records.person.data.PersonStatusDataRecord;
import dk.magenta.datafordeler.statistik.utils.Filter;

import javax.servlet.http.HttpServletRequest;

public class PersonDeathQuery extends PersonStatisticsQuery {

    public PersonDeathQuery(HttpServletRequest request) {
        super(request);
    }

    public PersonDeathQuery(Filter filter) {
        super(filter);
    }

    @Override
    public BaseLookupDefinition getLookupDefinition() {
        BaseLookupDefinition lookupDefinition = super.getLookupDefinition();

        FieldDefinition fieldDefinition = new FieldDefinition(
                PersonEntity.DB_FIELD_STATUS + LookupDefinition.separator + PersonStatusDataRecord.DB_FIELD_STATUS,
                90,
                Integer.class
        );

        this.applyOriginTimes(fieldDefinition);
        this.applyRegistrationTimes(fieldDefinition);
        this.applyEffectTimes(fieldDefinition);

        lookupDefinition.put(fieldDefinition);

        return lookupDefinition;
    }

}
