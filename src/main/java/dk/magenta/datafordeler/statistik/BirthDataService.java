package dk.magenta.datafordeler.statistik;

/*Birth data service Extract the following for a person:
    own pnr
    own birth year
    own effective pnr
    own birth municipality code (data missing, import handled in another ticket)
    own status code
    own prod date (to be investigated)
    mother's pnr
    mother's birth municipality code (data missing, import handled in another ticket)
    mother's status code
    mother's municipality code
    mother's locality name
    mother's road code
    mother's house number
    mother's door/apartment no.
    mother's bnr
    father's pnr
    father's birth municipality code (data missing, import handled in another ticket)
    father's status code
    father's municipality code
    father's locality name
    father's road code
    father's house number
    father's door/apartment no.
    father's bnr

Input parameters:
    birth year
    registration before date
    */

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/birth_data")
public class BirthDataService {

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }


//    @SpringBootApplication
//    public static class Application {
//
//        public static void main(String[] args) {
//            SpringApplication.run(Application.class, args);
//
//            System.out.println("Hi there");
//        }
//    }
}
