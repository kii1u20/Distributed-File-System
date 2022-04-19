import java.io.*;
import java.net.*;

/**
 * Controller
 */
public class Controller {

    public static void main(String[] args) {
        int cport = Integer.parseInt(args[0]);
        int repFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int reb_period = Integer.parseInt(args[3]);
        System.out.println("port: " + cport + ", " + "replication factor: " +
                repFactor + ", " + "timeout: " + timeout + ", " + "rebalance period: " + reb_period);

        ServerSocket ss = null;
        try {
            ss = new ServerSocket(cport);

            while (true) {
                System.out.println("Waiting for connection request...");

                final Socket client = ss.accept();

                System.out.println("Connected to: " + client.getInetAddress() + " on port: " + client.getPort());

                BufferedReader inStr = new BufferedReader(
                        new InputStreamReader(client.getInputStream()));
                String line;
                while ((line = inStr.readLine()) != null) {
                    System.out.println(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}