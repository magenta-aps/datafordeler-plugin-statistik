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
        civilStatus = filter.civilStatus;
    }

    @Override
    public BaseLookupDefinition getLookupDefinition() {
        BaseLookupDefinition lookupDefinition = super.getLookupDefinition();

        FieldDefinition fieldDefinition = new FieldDefinition(
                PersonEntity.DB_FIELD_CIVILSTATUS + LookupDefinition.separator + CivilStatusDataRecord.DB_FIELD_CIVIL_STATUS,
                civilStatus,
                String.class
        );

        this.applyOriginTimes(fieldDefinition);
        this.applyRegistrationTimes(fieldDefinition);
        this.applyEffectTimes(fieldDefinition);

        lookupDefinition.put(fieldDefinition);

        return lookupDefinition;
    }

}
