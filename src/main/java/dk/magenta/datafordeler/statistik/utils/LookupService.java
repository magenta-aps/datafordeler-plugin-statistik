package dk.magenta.datafordeler.statistik.utils;

import dk.magenta.datafordeler.core.database.Identification;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.util.DoubleHashMap;
import dk.magenta.datafordeler.cpr.data.road.data.RoadBaseData;
import dk.magenta.datafordeler.cpr.data.road.data.RoadCoreData;
import dk.magenta.datafordeler.cpr.data.road.data.RoadPostcodeData;
import dk.magenta.datafordeler.cpr.data.unversioned.PostCode;
import dk.magenta.datafordeler.cvr.data.unversioned.Municipality;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressData;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressEffect;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressEntity;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressQuery;
import dk.magenta.datafordeler.gladdrreg.data.bnumber.BNumberData;
import dk.magenta.datafordeler.gladdrreg.data.bnumber.BNumberEffect;
import dk.magenta.datafordeler.gladdrreg.data.bnumber.BNumberEntity;
import dk.magenta.datafordeler.gladdrreg.data.bnumber.BNumberQuery;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityData;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEffect;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityData;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEffect;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityQuery;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeData;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEffect;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEntity;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadData;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEffect;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEntity;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadQuery;
import org.hibernate.Session;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LookupService {

    public static final boolean COMPENSATE_2018_MUNICIPALITY_SPLIT = true;

    private Session session;

    private static Pattern houseNumberPattern = Pattern.compile("(\\d+)(.*)");

    public LookupService(Session session) {
        this.session = session;
    }

    private DoubleHashMap<Integer, Integer, Lookup> cache = new DoubleHashMap<>();

    public Lookup doLookup(int municipalityCode, int roadCode) {
        return this.doLookup(municipalityCode, roadCode, null);
    }

    public Lookup doLookup(int municipalityCode, int roadCode, String houseNumber) {

        Lookup lookup = this.cache.get(municipalityCode, roadCode);
        if (lookup == null) {
            lookup = new Lookup();
            this.cache.put(municipalityCode, roadCode, lookup);

            OffsetDateTime now = OffsetDateTime.now();

            if (municipalityCode >= 950) {

                MunicipalityEntity municipalityEntity = this.getMunicipalityGR(session, municipalityCode);
                if (municipalityEntity != null) {
                    for (MunicipalityEffect municipalityEffect : municipalityEntity.getRegistrationAt(now).getEffectsAt(now)) {
                        for (MunicipalityData municipalityData : municipalityEffect.getDataItems()) {
                            if (municipalityData.getName() != null) {
                                lookup.municipalityName = municipalityData.getName();
                                break;
                            }
                        }
                        if (lookup.municipalityName != null) break;
                    }

                    RoadEntity roadEntity = this.getRoadGR(session, municipalityEntity, roadCode);
                    if (roadEntity == null) {
                        this.populateRoadDK(lookup, session, municipalityCode, roadCode, houseNumber);
                    } else {
                        for (RoadEffect roadEffect : roadEntity.getRegistrationAt(now).getEffectsAt(now)) {
                            for (RoadData roadData : roadEffect.getDataItems()) {
                                if (roadData.getName() != null) {
                                    lookup.roadName = roadData.getName();
                                    break;
                                }
                            }
                            if (lookup.roadName != null) break;
                        }

                        AddressEntity addressEntity = this.getAddressGR(session, roadEntity, houseNumber);
                        if (addressEntity != null) {
                            BNumberEntity bNumberEntity = this.getBNumberGR(session, addressEntity);
                            if (bNumberEntity != null) {
                                for (BNumberEffect bNumberEffect : bNumberEntity.getRegistrationAt(now).getEffectsAt(now)) {
                                    for (BNumberData bNumberData : bNumberEffect.getDataItems()) {
                                        if (bNumberData.getCode() != null) {
                                            lookup.bNumber = bNumberData.getCode();
                                            break;
                                        }
                                    }
                                    if (lookup.bNumber != null) break;
                                }
                            }
                        }

                        LocalityEntity localityEntity = this.getLocalityGR(session, roadEntity);
                        if (localityEntity != null) {
                            for (LocalityEffect localityEffect : localityEntity.getRegistrationAt(now).getEffectsAt(now)) {
                                for (LocalityData localityData : localityEffect.getDataItems()) {
                                    if (localityData.getCode() > 0) {
                                        lookup.localityCode = localityData.getCode();
                                        lookup.localityName = localityData.getName();
                                        lookup.localityAbbrev = localityData.getAbbrev();
                                        break;
                                    }
                                }
                                if (lookup.localityCode != 0) break;
                            }

                            if (lookup.localityCode == 600) {
                                this.populateRoadDK(lookup, session, municipalityCode, roadCode, houseNumber);
                            } else {
                                PostalCodeEntity postalCodeEntity = this.getPostalCodeGR(session, localityEntity);
                                if (postalCodeEntity != null) {
                                    for (PostalCodeEffect postalCodeEffect : postalCodeEntity.getRegistrationAt(now).getEffectsAt(now)) {
                                        for (PostalCodeData postalCodeData : postalCodeEffect.getDataItems()) {
                                            if (postalCodeData.getCode() > 0) {
                                                lookup.postalCode = postalCodeData.getCode();
                                                lookup.postalDistrict = postalCodeData.getName();
                                                break;
                                            }
                                        }
                                        if (lookup.postalCode != 0) break;
                                    }
                                }
                            }
                        }
                    }
                }
            } else {

                Municipality municipality = this.getMunicipalityDK(session, municipalityCode);
                if (municipality != null) {
                    lookup.municipalityName = municipality.getName();
                    if (lookup.municipalityName != null) {
                        lookup.municipalityName = lookup.municipalityName.substring(0, 1).toUpperCase() + lookup.municipalityName.substring(1).toLowerCase();
                    }
                }
                this.populateRoadDK(lookup, session, municipalityCode, roadCode, houseNumber);

            }
        }
        return lookup;
    }

    private void populateRoadDK(Lookup lookup, Session session, int municipalityCode, int roadCode, String houseNumber) {
        dk.magenta.datafordeler.cpr.data.road.RoadEntity roadEntity = this.getRoadDK(session, municipalityCode, roadCode);
        if (roadEntity != null) {
            lookup.roadName = this.getRoadNameDK(roadEntity);
            PostCode postCode = this.getRoadPostalCodeDK(roadEntity, houseNumber);
            if (postCode != null) {
                lookup.postalCode = postCode.getPostnummer();
                lookup.postalDistrict = postCode.getPostdistrikt();
            }
        }
    }

    private HashMap<Integer, Municipality> municipalityCacheDK = new HashMap<>();

    private Municipality getMunicipalityDK(Session session, int municipalityCode) {
        Municipality municipality;
        if (municipalityCacheDK.containsKey(municipalityCode)) {
            municipality = municipalityCacheDK.get(municipalityCode);
            if (municipality != null) {
                session.merge(municipality);
            }
            return municipality;
        }
        municipality = QueryManager.getItem(session, Municipality.class, Collections.singletonMap(Municipality.DB_FIELD_CODE, municipalityCode));
        if (municipality != null) {
            municipalityCacheDK.put(municipalityCode, municipality);
        }
        return municipality;
    }

    private dk.magenta.datafordeler.cpr.data.road.RoadEntity getRoadDK(Session session, int municipalityCode, int roadCode) {
        try {
            dk.magenta.datafordeler.cpr.data.road.RoadQuery roadQuery = new dk.magenta.datafordeler.cpr.data.road.RoadQuery();
            roadQuery.setVejkode(roadCode);
            roadQuery.addKommunekode(municipalityCode);
            List<dk.magenta.datafordeler.cpr.data.road.RoadEntity> roadEntities = QueryManager.getAllEntities(session, roadQuery, dk.magenta.datafordeler.cpr.data.road.RoadEntity.class);
            return roadEntities.get(0);
        } catch (IndexOutOfBoundsException | NullPointerException e) {
        }
        return null;
    }

    private String getRoadNameDK(dk.magenta.datafordeler.cpr.data.road.RoadEntity roadEntity) {
        OffsetDateTime now = OffsetDateTime.now();
        for (dk.magenta.datafordeler.cpr.data.road.RoadRegistration roadRegistration : roadEntity.getRegistrations()) {
            for (dk.magenta.datafordeler.cpr.data.road.RoadEffect roadEffect : roadRegistration.getEffectsAt(now)) {
                for (RoadBaseData roadData : roadEffect.getDataItems()) {
                    RoadCoreData coreData = roadData.getCoreData();
                    if (coreData != null) {
                        return coreData.getName();
                    }
                }
            }
        }
        return null;
    }

    private PostCode getRoadPostalCodeDK(dk.magenta.datafordeler.cpr.data.road.RoadEntity roadEntity, String houseNumber) {
        OffsetDateTime now = OffsetDateTime.now();
        for (dk.magenta.datafordeler.cpr.data.road.RoadRegistration roadRegistration : roadEntity.getRegistrations()) {
            for (dk.magenta.datafordeler.cpr.data.road.RoadEffect roadEffect : roadRegistration.getEffectsAt(now)) {
                for (RoadBaseData roadData : roadEffect.getDataItems()) {
                    if (houseNumber != null) {
                        Matcher m = houseNumberPattern.matcher(houseNumber);
                        if (m.find()) {
                            int numberPart = Integer.parseInt(m.group(1));
                            String letterPart = m.group(2).toLowerCase();
                            Set<RoadPostcodeData> postcodeData = roadData.getPostcodeData();
                            if (postcodeData != null && !postcodeData.isEmpty()) {
                                for (RoadPostcodeData postcode : postcodeData) {
                                    if (postcode.getPostCode() != null) {
                                        Matcher from = houseNumberPattern.matcher(postcode.getHouseNumberFrom());
                                        Matcher to = houseNumberPattern.matcher(postcode.getHouseNumberTo());
                                        if (from.find() && to.find()) {
                                            int fromNumber = Integer.parseInt(from.group(1));
                                            int toNumber = Integer.parseInt(to.group(1));
                                            if (fromNumber < numberPart && numberPart < toNumber) {
                                                return postcode.getPostCode();
                                            } else if (fromNumber == numberPart || numberPart == toNumber) {
                                                String fromLetter = from.group(2).toLowerCase();
                                                String toLetter = from.group(2).toLowerCase();
                                                if ((fromNumber < numberPart || fromLetter.isEmpty() || fromLetter.compareTo(letterPart) <= 0) && (numberPart < toNumber || toLetter.isEmpty() || letterPart.compareTo(toLetter) < 0)) {
                                                    return postcode.getPostCode();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            Set<RoadPostcodeData> postcodeData = roadData.getPostcodeData();
                            if (postcodeData != null && !postcodeData.isEmpty()) {
                                for (RoadPostcodeData postcode : postcodeData) {
                                    if (postcode.getPostCode() != null) {
                                        return postcode.getPostCode();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private HashMap<Integer, MunicipalityEntity> municipalityCacheGR = new HashMap<>();

    private MunicipalityEntity getMunicipalityGR(Session session, int municipalityCode) {
        if (COMPENSATE_2018_MUNICIPALITY_SPLIT && (municipalityCode == 959 || municipalityCode == 960)) {
            municipalityCode = 958;
        }
        MunicipalityEntity municipalityEntity;
        if (municipalityCacheGR.containsKey(municipalityCode)) {
            municipalityEntity = municipalityCacheGR.get(municipalityCode);
            if (municipalityEntity != null) {
                session.merge(municipalityEntity);
            }
            return municipalityEntity;
        }
        try {
            MunicipalityQuery municipalityQuery = new MunicipalityQuery();
            municipalityQuery.setCode(Integer.toString(municipalityCode));
            List<MunicipalityEntity> municipalityEntities = QueryManager.getAllEntities(session, municipalityQuery, MunicipalityEntity.class);
            municipalityEntity = municipalityEntities.get(0);
            this.municipalityCacheGR.put(municipalityCode, municipalityEntity);
            return municipalityEntity;
        } catch (IndexOutOfBoundsException e) {
            this.municipalityCacheGR.put(municipalityCode, null);
        }
        return null;
    }

    private RoadEntity getRoadGR(Session session, MunicipalityEntity municipalityEntity, int roadCode) {
        try {
            RoadQuery roadQuery = new RoadQuery();
            roadQuery.setCode(Integer.toString(roadCode));
            roadQuery.setMunicipalityIdentifier(municipalityEntity.getUUID().toString());
            List<RoadEntity> roadEntities = QueryManager.getAllEntities(session, roadQuery, RoadEntity.class);
            return roadEntities.get(0);
        } catch (IndexOutOfBoundsException | NullPointerException e) {
        }
        return null;
    }

    private AddressEntity getAddressGR(Session session, RoadEntity roadEntity, String houseNumber) {
        try {
            AddressQuery addressQuery = new AddressQuery();
            addressQuery.setRoad(roadEntity.getUUID().toString());
            addressQuery.setHouseNumber(houseNumber);
            List<AddressEntity> addressEntities = QueryManager.getAllEntities(session, addressQuery, AddressEntity.class);
            return addressEntities.get(0);
        } catch (IndexOutOfBoundsException | NullPointerException e) {
        }
        return null;
    }

    private BNumberEntity getBNumberGR(Session session, AddressEntity addressEntity) {
        OffsetDateTime now = OffsetDateTime.now();
        for (AddressEffect addressEffect : addressEntity.getRegistrationAt(now).getEffectsAt(now)) {
            for (AddressData addressData : addressEffect.getDataItems()) {
                Identification locationIdentification = addressData.getbNumber();
                if (locationIdentification != null) {
                    BNumberEntity bNumber = QueryManager.getEntity(session, locationIdentification, BNumberEntity.class);
                    if (bNumber != null) {
                        return bNumber;
                    }
                }
            }
        }
        return null;
    }

    private LocalityEntity getLocalityGR(Session session, RoadEntity roadEntity) {
        OffsetDateTime now = OffsetDateTime.now();
        for (RoadEffect roadEffect : roadEntity.getRegistrationAt(now).getEffectsAt(now)) {
            for (RoadData roadData : roadEffect.getDataItems()) {
                Identification locationIdentification = roadData.getLocation();
                if (locationIdentification != null) {
                    LocalityEntity locality = QueryManager.getEntity(session, locationIdentification, LocalityEntity.class);
                    if (locality != null) {
                        return locality;
                    }
                }
            }
        }
        return null;
    }

    private PostalCodeEntity getPostalCodeGR(Session session, LocalityEntity localityEntity) {
        OffsetDateTime now = OffsetDateTime.now();
        for (LocalityEffect localityEffect : localityEntity.getRegistrationAt(now).getEffectsAt(now)) {
            for (LocalityData localityData : localityEffect.getDataItems()) {
                Identification postalCodeIdentification = localityData.getPostalCode();
                if (postalCodeIdentification != null) {
                    PostalCodeEntity postalCode = QueryManager.getEntity(session, postalCodeIdentification, PostalCodeEntity.class);
                    if (postalCode != null) {
                        return postalCode;
                    }
                }
            }
        }
        return null;
    }

}
