import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Controller
 */
public class Controller {

    public static ArrayList<DstoreObject> dstores = new ArrayList<DstoreObject>();
    public static ArrayList<Index> index = new ArrayList<Index>();
    public static List<Index> syncIndex = Collections.synchronizedList(index); 

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
                                    CountDownLatch storeLatch = new CountDownLatch(repFactor);
                                    String[] attr = line.split(" ");
                                    String fileName = attr[1];
                                    int fileSize = Integer.parseInt(attr[2]);

                                    if (checkFileExists(fileName)) {
                                        out.println("ERROR_FILE_ALREADY_EXISTS");
                                        System.out.println("ERROR_FILE_ALREADY_EXISTS");
                                        continue;
                                    }

                                    ArrayList<DstoreObject> dS = new ArrayList<DstoreObject>();
                                    String toClient = "STORE_TO ";
                                    for (int i = 0; i < repFactor; i++) {
                                        DstoreObject obj = dstores.get(i); // TODO: may need to select Dstores with least amount of files
                                        dS.add(obj);
                                        toClient += obj.port + " ";
                                    }
                                    syncIndex.add(new Index(fileName, fileSize, "store in progress”", dS, storeLatch));
                                    
                                    out.println(toClient.stripTrailing());
                                    System.out.println(toClient.stripTrailing());
                                    if (!storeLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                                        System.err.println("The STORE operation timed out");
                                        syncIndex.remove(getByFileName(fileName)); //If getByFileName return null, file doesnt exist
                                    } else {
                                        synchronized(syncIndex) { //TODO: might be deadlocking(ONLY IN DEBUG, MIGHT BE VSCode debugger holding the thread)
                                            for (Index file : syncIndex) {
                                                if (file.filename.equals(fileName)) {
                                                    file.lifecycle = "store complete";
                                                }
                                            }
                                        }
                                        System.out.println("STORE_COMPLETE for file " + fileName);
                                        out.println("STORE_COMPLETE");
                                    }
                                } else if (line.contains("STORE_ACK ")) {
                                    String filename = line.split(" ")[1];
                                    synchronized(syncIndex) { //TODO: might be deadlocking(ONLY IN DEBUG, MIGHT BE VSCode debugger holding the thread)
                                        for (Index file : syncIndex) {
                                            if (file.filename.equals(filename)) {
                                                file.latch.countDown();
                                            }
                                        }
                                    }
                                }
                            }
                            // if a Dstore disconnects or a client disconnects
                            client.close();
                            if (DstoreObj != null) {
                                dstores.remove(DstoreObj);
                                System.out.println("Removed Dstore on port " + DstoreObj.port + " because it disconnected");
                                synchronized(syncIndex) {
                                    Iterator<Index> itr = syncIndex.iterator();
                                    while (itr.hasNext()) { // TODO: May not need to remove store in progress files, CHECK!
                                        Index file = itr.next();
                                        if (file.dStore.contains(DstoreObj) && file.dStore.size() == 1) {
                                            itr.remove();
                                            // System.out.println("Removed " + file.filename + " from the index because it's Dstore disconnected");
                                        } else if (file.dStore.contains(DstoreObj)) {
                                            file.dStore.remove(DstoreObj);
                                        }
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
        synchronized(syncIndex) {
            for (Index file : syncIndex) {
                if (file.lifecycle.equals("store complete")) {
                    result += file.filename + " ";
                }
            }
        }
        return result.stripTrailing();
    }

    private static boolean checkFileExists(String fileName) {
        synchronized(syncIndex) {
            for (Index file : syncIndex) {
                if (file.filename.equals(fileName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Index getByFileName(String fileName) {
        synchronized(syncIndex) {
            for (Index file : syncIndex) {
                if (file.filename.equals(fileName)) {
                    return file;
                }
            }
        }
        return null;
    }
}

class Index {
    public String filename;
    public String lifecycle;
    public ArrayList<DstoreObject> dStore;
    public int filesize;
    public CountDownLatch latch;

    public Index(String filename, int filesize, String lifecycle, ArrayList<DstoreObject> dStore, CountDownLatch latch) {
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