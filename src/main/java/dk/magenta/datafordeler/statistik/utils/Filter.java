package dk.magenta.datafordeler.statistik.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.statistik.services.StatisticsService;

import javax.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.util.*;

public class Filter {

    public OffsetDateTime effectAt;

    public OffsetDateTime after;

    public OffsetDateTime before;

    public OffsetDateTime registrationAfter;

    public OffsetDateTime registrationBefore;

    public OffsetDateTime livingInGreenlandAtDate;

    public List<String> onlyPnr;

    public Filter() {
    }


    public Filter(HttpServletRequest request) {
        this.registrationAfter = Query.parseDateTime(request.getParameter(StatisticsService.REGISTRATION_AFTER));
        this.registrationBefore = Query.parseDateTime(request.getParameter(StatisticsService.REGISTRATION_BEFORE));
        this.effectAt = Query.parseDateTime(request.getParameter(StatisticsService.EFFECT_DATE_PARAMETER));
        this.before = Query.parseDateTime(request.getParameter(StatisticsService.BEFORE_DATE_PARAMETER));
        this.after = Query.parseDateTime(request.getParameter(StatisticsService.AFTER_DATE_PARAMETER));
        this.livingInGreenlandAtDate = Query.parseDateTime(request.getParameter(StatisticsService.INCLUSION_DATE_PARAMETER));
        String[] pnr = request.getParameterValues("pnr");
        if (pnr != null && pnr.length > 0) {
            this.onlyPnr = Arrays.asList(pnr);
        }
    }

    public Filter(ObjectNode node) {
        if (node.has(StatisticsService.REGISTRATION_AFTER)) {
            this.registrationAfter = Query.parseDateTime(node.get(StatisticsService.REGISTRATION_AFTER).asText());
        }
        if (node.has(StatisticsService.REGISTRATION_BEFORE)) {
            this.registrationBefore = Query.parseDateTime(node.get(StatisticsService.REGISTRATION_BEFORE).asText());
        }
        if (node.has(StatisticsService.EFFECT_DATE_PARAMETER)) {
            this.effectAt = Query.parseDateTime(node.get(StatisticsService.EFFECT_DATE_PARAMETER).asText());
        }
        if (node.has(StatisticsService.BEFORE_DATE_PARAMETER)) {
            this.before = Query.parseDateTime(node.get(StatisticsService.BEFORE_DATE_PARAMETER).asText());
        }
        if (node.has(StatisticsService.AFTER_DATE_PARAMETER)) {
            this.after = Query.parseDateTime(node.get(StatisticsService.AFTER_DATE_PARAMETER).asText());
        }
        if (node.has(StatisticsService.INCLUSION_DATE_PARAMETER)) {
            this.livingInGreenlandAtDate = Query.parseDateTime(node.get(StatisticsService.INCLUSION_DATE_PARAMETER).asText());
        }
        if (node.has(StatisticsService.INCLUSION_DATE_PARAMETER)) {
            JsonNode pnr = node.get("pnr");
            if (pnr != null) {
                ArrayList<String> pnrs = new ArrayList<>();
                if (pnr.isArray()) {
                    ArrayNode pnrA = (ArrayNode) pnr;
                    for (JsonNode p : pnrA) {
                        if (p.isValueNode()) {
                            pnrs.add(p.asText());
                        }
                    }
                } else if (pnr.isValueNode()) {
                    pnrs.add(pnr.asText());
                }
                if (!pnrs.isEmpty()) {
                    this.onlyPnr = pnrs;
                }
            }
        }
    }

    public Filter(OffsetDateTime effectAt) {
        this.effectAt = effectAt;
    }
}
