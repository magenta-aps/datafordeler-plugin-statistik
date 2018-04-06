package dk.magenta.datafordeler.statistik;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class StatistikApplication {

    public static void main(String[] args) {
        SpringApplication.run(StatistikApplication.class, args);

        System.out.println("Hi there");
    }
}
