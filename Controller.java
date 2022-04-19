import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Iterator;

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
                                System.out.println("Command: " + line);
                                if (line.contains("JOIN")) {
                                    int dPort = Integer.parseInt(line.split(" ")[1]);
                                    dstores.add((DstoreObj = new DstoreObject(client, dPort)));
                                    out.println("LIST");
                                    line = inStr.readLine();
                                    System.out.println("Dstore files: " + line);
                                    if (line.contains("LIST")) {
                                        line = line.replace("LIST ", "");
                                        String[] files = line.split(" ");
                                        for (String file : files) {
                                            index.add(new Index(file, "available", DstoreObj));
                                            System.out.println("Added " + file + " to the index");
                                        }
                                    }
                                }
                                if (dstores.size() < repFactor) {
                                    continue;
                                }
                                if (line.equals("LIST")) {
                                    System.out.println("List files requested: " + listFiles());
                                    out.println("LIST " + listFiles());
                                }
                                System.out.println("If there are enough dstores continue");
                            }
                            // May be issue, don't know how java sockets work
                            client.close();
                            if (DstoreObj != null) {
                                dstores.remove(DstoreObj);
                                System.out.println("Removed " + DstoreObj.port + " because it disconnected");
                                Iterator<Index> itr = index.iterator();
                                while (itr.hasNext()) {
                                    Index file = itr.next();
                                    if (file.dStore == DstoreObj) {
                                        itr.remove();
                                        System.out.println("Removed " + file.filename
                                                + " from the index because it's Dstore disconnected");
                                    }
                                }
                            } else {
                                System.out.println("Client on port " + client.getPort() + " disconnected");
                            }
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

    private static String listFiles() {
        String result = "";
        for (Index file : index) {
            if (file.lifecycle.equals("available")) {
                result += file.filename + " ";
            }
        }
        return result.stripTrailing();
    }
}

class Index {
    public String filename;
    public String lifecycle;
    public DstoreObject dStore;

    public Index(String filename, String lifecycle, DstoreObject dStore) {
        this.filename = filename;
        this.lifecycle = lifecycle;
        this.dStore = dStore;
    }
}

class DstoreObject {
    public Socket socket;
    public int port;

    public DstoreObject(Socket socket, int port) {
        this.socket = socket;
        this.port = port;
    }
}