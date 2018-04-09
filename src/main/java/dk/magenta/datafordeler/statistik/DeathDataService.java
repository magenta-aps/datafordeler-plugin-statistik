package dk.magenta.datafordeler.statistik;
/*Extract the following for a person:
    status code
    death date
    prod date (to be investigated)
    pnr
    birth year
    mother's pnr
    father's pnr
    spouse's pnr
    effective pnr
    status code
    birth municipality code (data missing, import handled in another ticket)
    municipality code
    locality name
    road code
    house number
    door/apartment no.
    bnr

Input parameters:
    death year
    registration before date
    registration after date
*/
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/death_data")
public class DeathDataService {

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }
}
