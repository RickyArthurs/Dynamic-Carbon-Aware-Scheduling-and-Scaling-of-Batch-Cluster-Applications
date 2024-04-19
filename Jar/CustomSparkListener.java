import org.apache.spark.scheduler.*;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
/* @author Richard Arthurs
 * This program is currently configured to work on local Windows machine.
 * This can be configured to work on linux cluster by commenting the windows option and uncommenting the linux ones
 * these reside in the constructor, onStageCompleted method and onJobEnd method.
 * In both cases the program must be ran with admin/sudo permissions
 */

public class CustomSparkListener extends SparkListener {
    private int stageCompletedCounter = 0;
    private String powerScheme;
    private double MAX_FREQ;
    private double HARDWARE_LIMIT;

    Map<Integer, Double> utilMap = new HashMap<Integer, Double>();

    /* Power Scheme GUID: 381b4222-f694-41f0-9685-ff5bb260df2e  (Balanced) * 30-50
     * Power Scheme GUID: 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c  (High performance) >50
     * Power Scheme GUID: a065b44e-cd9d-4119-bf1c-4f3fc869478a  (Ultimate Performance) >70
     * Power Scheme GUID: a1841308-3541-4fab-bc81-f71556f20b4a  (Power saver) <30
     * powercfg /setactive 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
    */
    NavigableMap<Integer, String> powerSchemeMap = new TreeMap<>();

    public CustomSparkListener(){
        // example CPU stress profile (intelligent DVFS plan)
        utilMap.put(0, 71.41439688715953);
        utilMap.put(1, 56.62777777777778);
        utilMap.put(2, 13.166666666666666);
        utilMap.put(3, 67.808);
        utilMap.put(4, 25.138388123011666);
        utilMap.put(5, 12.0);
        utilMap.put(6, 46.00487804878049);
        utilMap.put(7, 34.125);
        utilMap.put(8, 51.25);
        utilMap.put(9, 43.63398692810458);
        utilMap.put(10, 30.0);
        utilMap.put(11, 53.791811846689896);
        utilMap.put(12, 7.669354838709677);


        // Define the ranges and their corresponding values
        powerSchemeMap.put(30, "381b4222-f694-41f0-9685-ff5bb260df2e");  // Balanced
        powerSchemeMap.put(50, "8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c");  // High performance
        powerSchemeMap.put(70, "a065b44e-cd9d-4119-bf1c-4f3fc869478a");  // Ultimate Performance
        powerSchemeMap.put(Integer.MIN_VALUE, "a1841308-3541-4fab-bc81-f71556f20b4a"); // Power saver (for values less than 30)

        // for the Linux Cluster the implementation is much more simple
        // HARDWARE_LIMIT = 1.2;
        // MAX_FREQ = 2.5;
        // sudo cpupower frequency-set -f {value}GHz
        // where value is the CPUstress * maximum frequency

    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        System.out.println("Stage " + stageSubmitted.stageInfo().stageId() + " started");
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        System.out.println("Stage " + stageCompleted.stageInfo().stageId() + " completed." + "Profiler stage id: " + stageCompletedCounter);
        powerScheme = getPowerSchemeForUtil(utilMap.get(stageCompletedCounter));
        try {
            // //linux config:
            // double desired_frequency = utilMap.get(stageCompletedCounter) * MAX_FREQ;
            // if ((utilMap.get(stageCompletedCounter) * MAX_FREQ) < HARDWARE_LIMIT)
            // {
            //     // set to minimum frequency
            //     ProcessBuilder builder = new ProcessBuilder("sudo", "cpupower", "frequency-set", "-f", HARDWARE_LIMIT + "GHz");
            // }else
            // {
            //     ProcessBuilder builder = new ProcessBuilder("sudo", "cpupower", "frequency-set", "-f", desired_frequency + "GHz");
            // }
            //windows config
            ProcessBuilder builder = new ProcessBuilder("powercfg", "/setactive", powerScheme);
            builder.inheritIO(); // Redirects the input, output, and error streams of the subprocess to the Java process
            Process process = builder.start();
            int exitCode = process.waitFor(); // Waits for the process to terminate and returns the exit code
            if (exitCode == 0) {
                System.out.println("Power scheme set to: " + powerScheme);
            } else {
                System.err.println("Failed to set power scheme");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        stageCompletedCounter++;
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd){
        System.out.println("Job Completed");
        // windows balanced power plan to return to after execution
        powerScheme = "381b4222-f694-41f0-9685-ff5bb260df2e";
        // linux command for removing current frequency setting and returning system to normal
        // powerScheme = "sudo cpupower frequency-set"
        try {
            // linux config
            // ProcessBuilder builder = new ProcessBuilder(powerScheme);
            // windows config
            ProcessBuilder builder = new ProcessBuilder("powercfg", "/setactive", powerScheme);
            builder.inheritIO(); // Redirects the input, output, and error streams of the subprocess to the Java process
            Process process = builder.start();
            int exitCode = process.waitFor(); // Waits for the process to terminate and returns the exit code
            if (exitCode == 0) {
                System.out.println("Power scheme set to: " + powerScheme);
            } else {
                System.err.println("Failed to set power scheme");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public String getPowerSchemeForUtil(double util) {
        Integer floorKey = powerSchemeMap.floorKey((int) util);
        return floorKey != null ? powerSchemeMap.get(floorKey) : null;
    }
}