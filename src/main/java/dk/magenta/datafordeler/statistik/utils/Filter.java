package dk.magenta.datafordeler.statistik.utils;

import java.time.OffsetDateTime;

public class Filter {
    public OffsetDateTime effectAt;

    public Filter(OffsetDateTime effectAt) {
        this.effectAt = effectAt;
    }


    public OffsetDateTime after;

    public OffsetDateTime before;
}
