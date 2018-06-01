package dk.magenta.datafordeler.statistik.utils;

import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.statistik.services.StatisticsService;

import javax.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;

public class Filter {

    public OffsetDateTime effectAt;

    public OffsetDateTime after;

    public OffsetDateTime before;

    public OffsetDateTime registrationAfter;

    public Filter() {
    }

    public Filter(HttpServletRequest request) {
        this.registrationAfter = Query.parseDateTime(request.getParameter(StatisticsService.REGISTRATION_AFTER));
        this.effectAt = Query.parseDateTime(request.getParameter(StatisticsService.EFFECT_DATE_PARAMETER));
        this.before = Query.parseDateTime(request.getParameter(StatisticsService.BEFORE_DATE_PARAMETER));
        this.after = Query.parseDateTime(request.getParameter(StatisticsService.AFTER_DATE_PARAMETER));
    }

    public Filter(OffsetDateTime effectAt) {
        this.effectAt = effectAt;
    }
}
