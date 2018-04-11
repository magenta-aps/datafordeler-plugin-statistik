package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.statistik.services.MovementDataService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MovementDataServiceTest {

    private MovementDataService movementDataService;

    @Test
    public void greetingTest(){
        movementDataService = new MovementDataService();
        assertThat("All good in here...", is(equalTo(movementDataService.greeting())));
    }
}
