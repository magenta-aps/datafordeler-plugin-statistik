package dk.magenta.datafordeler.statistik;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/statistik/death_data")
public class DeathDataService {

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }
}
