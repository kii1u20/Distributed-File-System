import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Controller
 */
public class Controller {

    public static ArrayList<DstoreObject> dstores = new ArrayList<DstoreObject>();
    public static ArrayList<Index> index = new ArrayList<Index>();
    static CountDownLatch storeLatch; //TODO: may not work correctly for mutiple clients, need more testing

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

            // ArrayList<CountDownLatch> storeLatch = new ArrayList<CountDownLatch>(); //TODO: won't work for multiple clients connecting

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
                                    System.out.println("Dstore joined the system on port " + client.getPort());
                                    // } else if (line.contains("LIST ")) {
                                    // line = line.replace("LIST ", "");
                                    // System.out.println("Dstore files: " + line);
                                    // String[] files = line.split(" ");
                                    // for (String file : files) {
                                    // index.add(new Index(file, "available", DstoreObj));
                                    // System.out.println("Added " + file + " to the index");
                                    // }
                                } else if (dstores.size() < repFactor) {
                                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    System.out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    continue;
                                } else if (line.equals("LIST")) {
                                    System.out.println("List files requested: " + listFiles());
                                    out.println("LIST " + listFiles());
                                } else if (line.contains("STORE ")) {
                                    storeLatch = new CountDownLatch(repFactor);
                                    String[] attr = line.split(" ");
                                    String fileName = attr[1];
                                    int fileSize = Integer.parseInt(attr[2]);

                                    if (checkFileExists(fileName)) {
                                        out.println("ERROR_FILE_ALREADY_EXISTS");
                                        continue;
                                    }

                                    ArrayList<DstoreObject> dS = new ArrayList<DstoreObject>();
                                    String toClient = "STORE_TO ";
                                    for (int i = 0; i < repFactor; i++) {
                                        DstoreObject obj = dstores.get(i); // TODO: I want to select random Dstores, may implement later
                                        dS.add(obj);
                                        toClient += obj.port + " ";
                                    }
                                    index.add(new Index(fileName, fileSize, "store in progress”", dS));
                                    out.println(toClient.stripTrailing());
                                    System.out.println(toClient.stripTrailing());
                                    if (!storeLatch.await(30, TimeUnit.SECONDS)) {
                                        System.err.println("The STORE operation timed out");
                                    } else {
                                        for (Index file : index) {
                                            if (file.filename.equals(fileName)) {
                                                file.lifecycle = "store complete";
                                            }
                                        }
                                        System.out.println("STORE_COMPLETE for file " + fileName);
                                        out.println("STORE_COMPLETE");
                                    }
                                } else if (line.contains("STORE_ACK ")) {
                                    storeLatch.countDown();
                                    System.out.println("Latch count: " + storeLatch.getCount());
                                }
                            }
                            // if a Dstore disconnects or a client disconnects
                            client.close();
                            if (DstoreObj != null) {
                                dstores.remove(DstoreObj);
                                System.out.println("Removed Dstore on port " + DstoreObj.port + " because it disconnected");
                                Iterator<Index> itr = index.iterator();
                                while (itr.hasNext()) { // TODO: May not need to remove store in progress files, CHECK!
                                    Index file = itr.next();
                                    if (file.dStore.contains(DstoreObj) && file.dStore.size() == 1) {
                                        itr.remove();
                                        System.out.println("Removed " + file.filename + " from the index because it's Dstore disconnected");
                                    } else if (file.dStore.contains(DstoreObj)) {
                                        file.dStore.remove(DstoreObj);
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
            if (file.lifecycle.equals("store complete")) {
                result += file.filename + " ";
            }
        }
        return result.stripTrailing();
    }

    private static boolean checkFileExists(String fileName) {
        for (Index file : index) {
            if (file.filename.equals(fileName)) {
                return true;
            }
        }
        return false;
    }
}

class Index {
    public String filename;
    public String lifecycle;
    public ArrayList<DstoreObject> dStore;
    public int filesize;

    public Index(String filename, int filesize, String lifecycle, ArrayList<DstoreObject> dStore) {
        this.filename = filename;
        this.filesize = filesize;
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