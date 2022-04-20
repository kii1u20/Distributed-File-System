import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Controller
 */
public class Controller {

    // public static ArrayList<DstoreObject> dstores = new ArrayList<DstoreObject>();
    public static ConcurrentHashMap<Integer, DstoreObject> dstores = new ConcurrentHashMap<Integer, DstoreObject>();
    public static ConcurrentHashMap<String, Index> index = new ConcurrentHashMap<String, Index>();
    // public static ArrayList<Index> index = new ArrayList<Index>();
    // public static List<Index> syncIndex = Collections.synchronizedList(index); //TODO: can switch to using hashtable, which is threadsafe
                                                                               //https://www.geeksforgeeks.org/hashtable-in-java/

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
                                    dstores.put(dPort, (DstoreObj = new DstoreObject(client, dPort)));
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
                                    CountDownLatch storeLatch = new CountDownLatch(repFactor);
                                    String[] attr = line.split(" ");
                                    String fileName = attr[1];
                                    int fileSize = Integer.parseInt(attr[2]);

                                    if (index.get(fileName) != null) {
                                        out.println("ERROR_FILE_ALREADY_EXISTS");
                                        System.out.println("ERROR_FILE_ALREADY_EXISTS");
                                        continue;
                                    }

                                    ConcurrentHashMap<Integer, DstoreObject> dS = new ConcurrentHashMap<Integer, DstoreObject>();
                                    String toClient = "STORE_TO ";
                                    for (int i = 0; i < repFactor; i++) {
                                        Integer key = (Integer) dstores.keySet().toArray()[i]; //TODO: EDIT!
                                        DstoreObject obj = dstores.get(key); // TODO: may need to select Dstores with least amount of files
                                        dS.put(key, obj);
                                        toClient += obj.port + " ";
                                    }
                                    index.put(fileName, new Index(fileName, fileSize, "store in progress”", dS, storeLatch));
                                    
                                    out.println(toClient.stripTrailing());
                                    System.out.println(toClient.stripTrailing());
                                    if (!storeLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                                        System.err.println("The STORE operation timed out");
                                        index.remove(fileName); //If getByFileName return null, file doesnt exist
                                    } else {
                                        index.get(fileName).lifecycle = "store complete";
                                        System.out.println("STORE_COMPLETE for file " + fileName);
                                        out.println("STORE_COMPLETE");
                                    }
                                } else if (line.contains("STORE_ACK ")) {
                                    String filename = line.split(" ")[1];
                                    index.get(filename).latch.countDown();
                                    System.out.println(filename + " latch: " + index.get(filename).latch.getCount());
                                } else if (line.contains("LOAD ")) {
                                    String filename = line.split(" ")[1];

                                }
                            }
                            // if a Dstore disconnects or a client disconnects
                            client.close();
                            if (DstoreObj != null) {
                                dstores.remove(DstoreObj.port);
                                System.out.println("Removed Dstore on port " + DstoreObj.port + " because it disconnected");
                                for (Index file : index.values()) {
                                    if (file.dStore.containsValue(DstoreObj) && file.dStore.size() == 1) {
                                        index.remove(file.filename);
                                    } else if (file.dStore.contains(DstoreObj)) {
                                        file.dStore.remove(DstoreObj.port);
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
        var files = index.values();
        for (Index file : files) {
            result += file.filename + " ";
        }
        return result.stripTrailing();
    }
}

class Index {
    public String filename;
    public String lifecycle;
    public ConcurrentHashMap<Integer, DstoreObject> dStore;
    public int filesize;
    public CountDownLatch latch;

    public Index(String filename, int filesize, String lifecycle, ConcurrentHashMap<Integer, DstoreObject> dStore, CountDownLatch latch) {
        this.filename = filename;
        this.filesize = filesize;
        this.lifecycle = lifecycle;
        this.dStore = dStore;
        this.latch = latch;
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