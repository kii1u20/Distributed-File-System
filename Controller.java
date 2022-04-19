import java.io.*;
import java.net.*;
import java.util.ArrayList;

/**
 * Controller
 */
public class Controller {

    public static ArrayList<DstoreObject> dstores = new ArrayList<DstoreObject>();
    public static ArrayList<Index> index = new ArrayList<Index>();

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

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("Connected to: " + client.getInetAddress()
                                + " on port: " + client.getPort());
                        try {
                            BufferedReader inStr = new BufferedReader(new InputStreamReader(client.getInputStream()));
                            PrintWriter out = new PrintWriter(client.getOutputStream(), true);

                            String line;
                            DstoreObject DstoreObj = null;
                            while ((line = inStr.readLine()) != null) {
                                System.out.println(line);
                                if (line.contains("JOIN")) {
                                    int dPort = Integer.parseInt(line.split(" ")[1]);
                                    dstores.add((DstoreObj = new DstoreObject(client, dPort)));
                                    out.println(dstores.size());
                                }
                                if (dstores.size() < repFactor) {
                                    continue;
                                }
                                System.out.println("If there are enough Dstores only then handle client events");
                            }
                            //May be issue, don't know how java sockets work
                            client.close();
                            if (DstoreObj != null)
                                dstores.remove(DstoreObj);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}