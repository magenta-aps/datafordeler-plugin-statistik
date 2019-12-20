package dk.magenta.datafordeler.statistik.reportExecution;

public class ReportRunner implements Runnable {

    private int counter;

    // Constructor
    public ReportRunner(int counter) {
        this.counter = counter;
    }

    @Override
    public void run() {
        while(counter>0) {
            System.out.println("RUN");
            counter--;
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            /*try {
                this.wait(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }
    }
}
