import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;

/**
 * Controller
 */
public class Controller {

    public static ConcurrentHashMap<Integer, DstoreObject> dstores = new ConcurrentHashMap<Integer, DstoreObject>();
    public static ConcurrentHashMap<String, Index> index = new ConcurrentHashMap<String, Index>();
    public static ConcurrentHashMap<String, ConcurrentHashMap<Integer, DstoreObject>> rebalanceFiles = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, DstoreObject>>();

    static AtomicInteger dStoresReplied = new AtomicInteger(0);
    static AtomicInteger rebalancesDone = new AtomicInteger(0);
    static AtomicBoolean isRebalancing = new AtomicBoolean(false);
    static AtomicBoolean pendingOp = new AtomicBoolean(false);

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

                            ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();

                            int reloadCount = 0;
                            String line;
                            DstoreObject DstoreObj = null;
                            while ((line = inStr.readLine()) != null) {
                                if (isRebalancing.get() == false) {
                                    System.out.println("Command: " + line);
                                    if (line.contains("JOIN")) {
                                        int dPort = Integer.parseInt(line.split(" ")[1]);
                                        dstores.put(dPort, (DstoreObj = new DstoreObject(client, dPort)));
                                        System.out.println("Dstore joined the system on port " + client.getPort());
                                        if (rebalancesDone.get() != 0) {
                                            
                                        }
                                        startRebalance(); //TODO: Move it up in the IF
                                    } else if (dstores.size() < repFactor) {
                                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                                        System.out.println("ERROR_NOT_ENOUGH_DSTORES");
                                        continue;
                                    } else if (line.equals("LIST")) {
                                        System.out.println("List files requested: " + listFiles());
                                        out.println("LIST " + listFiles());
                                    } else if (line.contains("STORE ")) {
                                        pendingOp.set(true);
                                        String[] attr = line.split(" ");
                                        String fileName = attr[1];
                                        int fileSize = Integer.parseInt(attr[2]);
                                        
                                        if (index.get(fileName) != null) {
                                            out.println("ERROR_FILE_ALREADY_EXISTS");
                                            System.out.println("ERROR_FILE_ALREADY_EXISTS");
                                            continue;
                                        }
    
                                        CountDownLatch storeLatch = new CountDownLatch(repFactor);
    
                                        ConcurrentHashMap<Integer, DstoreObject> dS = new ConcurrentHashMap<Integer, DstoreObject>();
                                        String toClient = "STORE_TO ";
                                        for (int i = 0; i < repFactor; i++) {
                                            Integer key = (Integer) dstores.keySet().toArray()[i]; //TODO: EDIT!
                                            DstoreObject obj = dstores.get(key); // TODO: may need to select Dstores with least amount of files
                                            dS.put(key, obj);
                                            toClient += obj.port + " ";
                                        }
                                        index.put(fileName, new Index(fileName, fileSize, "store in progress”", dS, storeLatch));
                                        Thread.sleep(10); //In case a client adds something, and the Dstore returns STORE_ACK before the Hashmap has updated
                                        out.println(toClient.stripTrailing());
                                        System.out.println(toClient.stripTrailing());
                                        if (!storeLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                                            System.err.println("The STORE operation timed out");
                                            index.remove(fileName);
                                        } else {
                                            index.get(fileName).lifecycle = "store complete";
                                            System.out.println("STORE_COMPLETE for file " + fileName);
                                            out.println("STORE_COMPLETE");
                                        }
                                        pendingOp.set(false);
                                    } else if (line.contains("STORE_ACK ")) {
                                        String filename = line.split(" ")[1];
                                        index.get(filename).latch.countDown();
                                        System.out.println(filename + " latch: " + index.get(filename).latch.getCount());
                                    } else if (line.contains("LOAD ")) {
                                        reloadCount = 1;
                                        String filename = line.split(" ")[1];
                                        Index file = index.get(filename);
                                        if (file == null || file.lifecycle != "store complete") {
                                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                                            System.out.println("ERROR_FILE_DOES_NOT_EXIST");
                                            continue;
                                        }
                                        Integer port = (Integer) file.dStore.keySet().toArray()[0];
                                        out.println("LOAD_FROM " + port + " " + file.filesize);
                                    } else if (line.contains("RELOAD ")) {
                                        String filename = line.split(" ")[1];
                                        if (reloadCount < repFactor) {
                                            Index file = index.get(filename);
                                            if (file == null || file.lifecycle != "store complete") {
                                                out.println("ERROR_FILE_DOES_NOT_EXIST");
                                                System.out.println("ERROR_FILE_DOES_NOT_EXIST");
                                                continue;
                                            }
                                            Integer port = (Integer) file.dStore.keySet().toArray()[reloadCount];
                                            out.println("LOAD_FROM " + port + " " + file.filesize);
                                            reloadCount++;
                                        } else {
                                            out.println("ERROR_LOAD");
                                            System.out.println("ERROR_LOAD");
                                        }
                                    } else if (line.contains("REMOVE ")) {
                                        pendingOp.set(true);
                                        String filename = line.split(" ")[1];
                                        Index file = index.get(filename);
                                        if (file == null || !file.lifecycle.equals("store complete")) {
                                            out.println("ERROR_FILE_DOES_NOT_EXIST");
                                            continue;
                                        }
                                        if (file.lifecycle.equals("store complete")) {
                                            file.lifecycle = "remove in progress";
                                            file.latch = new CountDownLatch(file.dStore.size());
                                            for (DstoreObject dstore : file.dStore.values()) {
                                                // BufferedReader dStoreIn = new BufferedReader(new InputStreamReader(dstore.socket.getInputStream()));
                                                PrintWriter dStoreOut = new PrintWriter(dstore.socket.getOutputStream(), true);
                                                dStoreOut.println("REMOVE " + filename);
                                            }
                                            if (file.latch.await(timeout, TimeUnit.MILLISECONDS)) {
                                                file.lifecycle = "remove complete";
                                                index.remove(filename);
                                                out.println("REMOVE_COMPLETE");
                                            }//TODO: May have to check if at least one Dstore has removed the file even if the latch fails
                                        } else {
                                            //TODO: ERROR - check concurrent operations - implement later
                                        }
                                        pendingOp.set(false);
                                    } else if (line.contains("REMOVE_ACK ")) {
                                        String filename = line.split(" ")[1];
                                        Index file = index.get(filename);
                                        file.latch.countDown();
                                    }
                                } else {
                                    if (line.contains("LIST ")) {
                                        dStoresReplied.incrementAndGet();
                                        line = line.replace("LIST ", "");
                                        System.out.println("Dstore files: " + line);
                                        String[] files = line.split(" ");
                                        for (String file : files) {
                                            if (!file.equals("")) {
                                                if (rebalanceFiles.get(file) == null) {
                                                    var temp = new ConcurrentHashMap<Integer, DstoreObject>();
                                                    temp.put(DstoreObj.port, DstoreObj);
                                                    rebalanceFiles.put(file, temp);
                                                } else {
                                                    rebalanceFiles.get(file).put(DstoreObj.port, DstoreObj);
                                                }
                                            }
                                        }
                                        if (dStoresReplied.get() == dstores.size()) {
                                            System.out.println(Thread.currentThread()); //TODO: Remove - DEBUG only
                                            Thread.sleep(5000); //TODO: Remove - DEBUG only
                                            rebalance();
                                        }
                                    } else if (line.equals("REBALANCE_COMPLETE")) {
                                        isRebalancing.set(false);
                                    } else {
                                        queue.add(line);
                                    }
                                }
                            }
                            // if a Dstore disconnects or a client disconnects
                            client.close();
                            if (DstoreObj != null) {
                                dstores.remove(DstoreObj.port);
                                System.out.println("Removed Dstore on port " + DstoreObj.port + " because it disconnected");
                                for (Index file : index.values()) {
                                    if (file.dStore.containsValue(DstoreObj) && file.dStore.size() == 1) { //TODO: Might need to check the size to replication factor
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

    private static void rebalance() throws Exception {
        rebalancesDone.incrementAndGet();

        System.out.println(Thread.currentThread()); //TODO: Remove - DEBUG only

        // Thread.sleep(10000); //TODO: remove, JUST for debugging
        for (Index file : index.values()) {
            if (rebalanceFiles.get(file.filename) == null) {
                index.remove(file.filename);
            }
            if (file.lifecycle.equals("remove in progress")) {
                for (DstoreObject dstore : file.dStore.values()) {
                    PrintWriter out = new PrintWriter(dstore.socket.getOutputStream(), true);
                    out.print("REMOVE " + file.filename);
                }
            }
        }
        isRebalancing.set(false); //TODO: Remove this
    }

    private static void startRebalance() throws Exception {
        while (pendingOp.get()) {
            //Nothing
        }
        PrintWriter out;
        dStoresReplied.set(0);
        rebalanceFiles.clear();
        isRebalancing.set(true);
        for (DstoreObject dStore : dstores.values()) {
            out = new PrintWriter(dStore.socket.getOutputStream(), true);
            out.println("LIST");
        }
    }

    private static String listFiles() {
        String result = "";
        var files = index.values();
        for (Index file : files) {
            if (file.lifecycle.equals("store complete")) {
                result += file.filename + " ";
            }
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