package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;

import javax.servlet.http.HttpServletRequest;
import java.util.HashSet;

public class PersonMoveQuery extends PersonStatisticsQuery {

    public PersonMoveQuery(HttpServletRequest request) {
        super(request);
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        FieldDefinition addressDefinition = this.fromPath(LookupDefinition.entityref + LookupDefinition.separator + PersonEntity.DB_FIELD_ADDRESS);
        FieldDefinition migrationDefinition = this.fromPath(LookupDefinition.entityref + LookupDefinition.separator + PersonEntity.DB_FIELD_FOREIGN_ADDRESS_EMIGRATION);

        addressDefinition.or(migrationDefinition);
        lookupDefinition.put(addressDefinition);

        return lookupDefinition;
    }

}
