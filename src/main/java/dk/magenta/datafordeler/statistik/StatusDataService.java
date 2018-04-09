package dk.magenta.datafordeler.statistik;
/*Extract the following for a person:
    pnr
    birth year
    firstname
    lastname
    status code
    birth municipality code (data missing, import handled in another ticket)
    mother's pnr
    father's pnr
    civil status
    spouse's pnr
    municipality code
    locality name
    road code
    house number
    floor
    door/apartment no.
    bnr
    moving-in date
    postcode
    civil status date
    church (to be investigated)

Input parameters:
    living in Greenland on date*/

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/status_data")
public class StatusDataService {

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }

}
