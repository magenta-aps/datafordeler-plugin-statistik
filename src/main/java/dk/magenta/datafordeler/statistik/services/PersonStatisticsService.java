package dk.magenta.datafordeler.statistik.services;

import dk.magenta.datafordeler.core.arearestriction.AreaRestriction;
import dk.magenta.datafordeler.core.arearestriction.AreaRestrictionType;
import dk.magenta.datafordeler.core.database.DatabaseEntry;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.plugin.AreaRestrictionDefinition;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.util.BitemporalityComparator;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprAreaRestrictionDefinition;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.cpr.records.CprBitemporalRecord;
import dk.magenta.datafordeler.cpr.records.CprBitemporality;
import dk.magenta.datafordeler.cpr.records.CprNontemporalRecord;
import dk.magenta.datafordeler.statistik.StatistikRolesDefinition;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.LookupService;
import org.hibernate.Session;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;

public abstract class PersonStatisticsService extends StatisticsService {

    public static final ZoneId cprDataOffset = ZoneId.of("Europe/Copenhagen");

    protected String[] requiredParameters() {
        return new String[]{};
    }

    protected abstract CprPlugin getCprPlugin();

    protected DafoUserDetails getUser(HttpServletRequest request) throws InvalidTokenException, AccessDeniedException, InvalidCertificateException {
        return this.getDafoUserManager().getUserFromRequest(request);
    }

    public int run(Filter filter, OutputStream outputStream) {

        final Session primarySession = this.getSessionManager().getSessionFactory().openSession();
        final Session secondarySession = this.getSessionManager().getSessionFactory().openSession();

        primarySession.setDefaultReadOnly(true);
        secondarySession.setDefaultReadOnly(true);

        try {
            List<PersonRecordQuery> queries = this.getQueryList(filter);
            Stream<Map<String, String>> concatenation = null;

            for (PersonRecordQuery query : queries) {
                //this.applyAreaRestrictionsToQuery(query, user);
                Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primarySession, query, PersonEntity.class);
                Stream<Map<String, String>> formatted = this.formatItems(primarySession, personEntities, secondarySession, filter);
                concatenation = (concatenation == null) ? formatted : Stream.concat(concatenation, formatted);
            }

            if (concatenation != null) {
                //final Counter counter = new Counter();
                if (outputStream != null) {
                    return this.writeItems(concatenation.iterator(), outputStream, item -> {
                        /*counter.count++;
                        if (counter.count > 100) {
                            primarySession.clear();
                            secondarySession.clear();
                            counter.count = 0;
                        }*/
                    });
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            primarySession.close();
            secondarySession.close();
        }
        return 0;
    }

    protected abstract List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter);

    protected PersonRecordQuery getQuery(Filter filter) {
        PersonRecordQuery personQuery = new PersonRecordQuery();
        /*if (filter.livingInGreenlandAtDate != null) {
            personQuery.setEffectFrom(filter.livingInGreenlandAtDate);
            personQuery.setEffectTo(filter.livingInGreenlandAtDate);
        }*/
        if (filter.onlyPnr != null) {
            for (String pnr : filter.onlyPnr) {
                personQuery.addPersonnummer(pnr);
            }
        }
        return personQuery;
    }

    protected List<PersonRecordQuery> getQueryList(Filter filter) throws IOException {
        return Collections.singletonList(this.getQuery(filter));
    }

    public Stream<Map<String, String>> formatItems(Session personSession, Stream<PersonEntity> personEntities, Session lookupSession, Filter filter) {
        LookupService lookupService = new LookupService(lookupSession);
        return personEntities.flatMap(
                personEntity -> {
                    List<Map<String, String>> output = formatPerson(personEntity, lookupSession, lookupService, filter);
                    personSession.evict(personEntity);
                    return output.stream();
                }
        );
    }

    @Override
    protected void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException {
        try {
            loggerHelper.getUser().checkHasSystemRole(CprRolesDefinition.READ_CPR_ROLE);
            loggerHelper.getUser().checkHasSystemRole(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        } catch (AccessDeniedException e) {
            loggerHelper.info("Access denied: " + e.getMessage());
            throw (e);
        }
    }

    protected void applyAreaRestrictionsToQuery(PersonRecordQuery query, DafoUserDetails user) throws InvalidClientInputException {
        Collection<AreaRestriction> restrictions = user.getAreaRestrictionsForRole(CprRolesDefinition.READ_CPR_ROLE);
        AreaRestrictionDefinition areaRestrictionDefinition = this.getCprPlugin().getAreaRestrictionDefinition();
        AreaRestrictionType municipalityType = areaRestrictionDefinition.getAreaRestrictionTypeByName(CprAreaRestrictionDefinition.RESTRICTIONTYPE_KOMMUNEKODER);
        for (AreaRestriction restriction : restrictions) {
            if (restriction.getType() == municipalityType) {
                query.addKommunekodeRestriction(restriction.getValue());
            }
        }
    }

    public static <R extends CprBitemporalRecord> Set<R> filterRecordsByEffect(Collection<R> records, OffsetDateTime effectAt) {
        HashSet<R> filtered = new HashSet<>();
        for (R record : records) {
            if (record.getBitemporality().containsEffect(effectAt, effectAt)) {
                filtered.add(record);
            }
        }
        return filtered;
    }

    public static <R extends CprBitemporalRecord> Set<R> filterRecordsByRegistration(Collection<R> records, OffsetDateTime registrationAt) {
        HashSet<R> filtered = new HashSet<>();
        for (R record : records) {
            if (record.getBitemporality().containsRegistration(registrationAt, registrationAt)) {
                filtered.add(record);
            }
        }
        return filtered;
    }

    public static <R extends CprBitemporalRecord> Set<R> filterUndoneRecords(Collection<R> records) {
        HashSet<R> filtered = new HashSet<>();
        for (R record : records) {
            if (!record.isUndone()) {
                filtered.add(record);
            }
        }
        return filtered;
    }

    private static Comparator bitemporalComparator = Comparator.comparing(PersonStatisticsService::getBitemporality, BitemporalityComparator.ALL)
            .thenComparing(CprNontemporalRecord::getDafoUpdated)
            .thenComparing(DatabaseEntry::getId);

    public static <R extends CprBitemporalRecord> List<R> sortRecords(Collection<R> records) {
        ArrayList<R> recordList = new ArrayList<>(records);
        recordList.sort(bitemporalComparator);
        return recordList;
    }




    /**
     * Find the most important registration according to "bitemporalComparator"
     * Records with a missing OriginDate is removed since they are considered invalid
     * @param records
     * @param <R>
     * @return
     */
    public static <R extends CprBitemporalRecord> R findMostImportant(Collection<R> records) {
        return (R) records.stream().filter(r -> r.getOriginDate()!=null).max(bitemporalComparator).orElse(null);
    }

    /**
     * Find the newest unclosed record from the list of records
     * Records with a missing OriginDate is also removed since they are considered invalid
     * @param records
     * @param <R>
     * @return
     */
    public static <R extends CprBitemporalRecord> R findNewestUnclosed(Collection<R> records) {
        return (R) records.stream().filter(r -> r.getBitemporality().registrationTo == null &&
                r.getOriginDate() != null).max(bitemporalComparator).orElse(null);
    }

    /**
     *
     * @param records
     * @param registrationAt
     * @param <R>
     * @return
     */
    public static <R extends CprBitemporalRecord> R findNewestAfterFilterOnEffect(Collection<R> records, OffsetDateTime registrationAt) {
        return (R) records.stream().filter(r -> r.getBitemporality().containsEffect(registrationAt, registrationAt) &&
                r.getOriginDate()!=null).max(bitemporalComparator).orElse(null);
    }



    public static CprBitemporality getBitemporality(CprBitemporalRecord record) {
        return record.getBitemporality();
    }

}
