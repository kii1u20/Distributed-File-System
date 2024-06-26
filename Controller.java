import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toMap;

/**
 * Controller
 */
public class Controller {

    static ConcurrentHashMap<Integer, DstoreObject> dstores = new ConcurrentHashMap<Integer, DstoreObject>();
    static ConcurrentHashMap<DstoreObject, CopyOnWriteArrayList<String>> dstoresFiles = new ConcurrentHashMap<DstoreObject, CopyOnWriteArrayList<String>>();
    static CopyOnWriteArrayList<DstoreRebalance> rebalance = new CopyOnWriteArrayList<DstoreRebalance>();
    static CopyOnWriteArrayList<String> rebalanceFiles = new CopyOnWriteArrayList<String>();
    static ConcurrentHashMap<String, Index> index = new ConcurrentHashMap<String, Index>();
    static ConcurrentHashMap<String, CopyOnWriteArrayList<DstoreObject>> filesToSend = new ConcurrentHashMap<String, CopyOnWriteArrayList<DstoreObject>>();

    static AtomicInteger dStoresReplied = new AtomicInteger(0);
    static AtomicInteger rebalancesDone = new AtomicInteger(0);
    static AtomicBoolean isRebalancing = new AtomicBoolean(false);
    static AtomicBoolean pendingOp = new AtomicBoolean(false);

    static CountDownLatch rebalanceLatch;
    static CountDownLatch rebalanceCompleteLatch;
    static ScheduledFuture<?> rebalanceTimer;

    static int cport;
    static int repFactor;
    static int timeout;
    static int reb_period;

    public static void main(String[] args) {
        cport = Integer.parseInt(args[0]);
        repFactor = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        reb_period = Integer.parseInt(args[3]);
        System.out.println("port: " + cport + ", " + "replication factor: " +
                repFactor + ", " + "timeout: " + timeout + ", " + "rebalance period: " + reb_period);

        ServerSocket ss = null;
        try {
            ss = new ServerSocket(cport);
            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

            Runnable runRebalance = new Runnable() {
                public void run() {
                    try {
                        if (dstores.size() < repFactor) {
                            System.err.println("ERROR_NOT_ENOUGH_DSTORES");
                        } else {
                            System.out.println("Rebalancing...");
                            startRebalance();
                        }
					} catch (Exception e) {
						e.printStackTrace();
					}
                }
            };

            rebalanceTimer = executorService.scheduleAtFixedRate(runRebalance, reb_period, reb_period, TimeUnit.SECONDS);
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
                            while (true) {
                                if ((line = inStr.readLine()) == null) {
                                    break;
                                }
                                if (isRebalancing.get() == false) {
                                    System.out.println("Command: " + line);
                                    if (line.contains("JOIN")) {
                                        int dPort = Integer.parseInt(line.split(" ")[1]);
                                        dstores.put(dPort, (DstoreObj = new DstoreObject(client, dPort)));
                                        dstoresFiles.put(DstoreObj, new CopyOnWriteArrayList<String>());
                                        System.out.println("Dstore joined the system on port " + client.getPort());
                                        if (rebalancesDone.get() != 0) {
                                            rebalanceTimer.cancel(false);
                                            System.out.println("Rebalance timer cancelled");
                                            rebalanceTimer = executorService.scheduleAtFixedRate(runRebalance, 0, reb_period, TimeUnit.SECONDS);
                                        }
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
                                            DstoreObject obj = getDstoreForStore(fileName);
                                            dstoresFiles.get(obj).add(fileName);
                                            dS.put(obj.port, obj);
                                            toClient += obj.port + " ";
                                        }
                                        index.put(fileName, new Index(fileName, fileSize, "store in progress”", dS, storeLatch));
                                        Thread.sleep(10); //In case a client adds something, and the Dstore returns STORE_ACK before the Hashmap has updated
                                                          //TODO: May need to remove this as it may not be needed
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
                                        System.out.println("Dstore " + DstoreObj.port + " files: " + line);
                                        String[] files = line.split(" ");
                                        List<String> filesList = Arrays.asList(files);
                                        //TODO: BELOW ARE WRONG!
                                        for (String file : files) {
                                            if (!file.equals("")) {
                                               rebalanceFiles.add(file);
                                            }
                                        }
                                        for (String file : dstoresFiles.get(DstoreObj)) {
                                            if (!filesList.contains(file)) {
                                                dstoresFiles.get(DstoreObj).remove(file);
                                            }
                                        }
                                        rebalanceLatch.countDown();
                                        System.out.println("Rebalance latch: " + rebalanceLatch.getCount());
                                    } else if (line.equals("REBALANCE_COMPLETE")) {
                                        rebalanceCompleteLatch.countDown();
                                    } else {
                                        System.out.println("QUEUE!");
                                        queue.add(line);
                                    }
                                }
                            }
                            // if a Dstore disconnects or a client disconnects
                            client.close();
                            if (DstoreObj != null) {
                                dstores.remove(DstoreObj.port);
                                dstoresFiles.remove(DstoreObj);
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

    private static void executeRebalance() throws Exception {
        for (DstoreObject ds : dstores.values()) {
            var list = getByDstore(ds);
            String result = "REBALANCE ";
            ConcurrentHashMap<String, CopyOnWriteArrayList<DstoreObject>> filesToSend = new ConcurrentHashMap<String, CopyOnWriteArrayList<DstoreObject>>();
            CopyOnWriteArrayList<String> filesToDelete = new CopyOnWriteArrayList<String>();
            if (list.isEmpty()) {
                System.out.println("No pending rebalancing for Dstore: " + ds.port);
                rebalanceCompleteLatch.countDown();
            } else {
                PrintWriter out = new PrintWriter(ds.socket.getOutputStream(), true);
                for (DstoreRebalance command : list) {
                    var initialDstore = command.initialDstore;
                    var targetDstores = command.targetDstores;
                    var operation = command.operation;
                    if (operation.equals(RebalanceOperation.SEND)) {
                        filesToSend.put(command.file, targetDstores);
                    } else {
                        filesToDelete.add(command.file);
                    }
                }
                result += filesToSend.size();
                for (String file : filesToSend.keySet()) {
                    result += " " + file + " " + filesToSend.get(file).size();
                    for (DstoreObject dstore : filesToSend.get(file)) {
                        result += " " + dstore.port;
                    } 
                }
                result += " " + filesToDelete.size();
                for (String file : filesToDelete) {
                    result +=  " " + file;
                }
                System.out.println(result);
                out.println(result);
            }
        }
        if (rebalanceCompleteLatch.await(timeout, TimeUnit.MILLISECONDS)) {
            isRebalancing.set(false);
        }
    }

    private static void rebalance() throws Exception {
        rebalancesDone.incrementAndGet();

        System.out.println(Thread.currentThread()); //TODO: Remove - DEBUG only

        for (Index file : index.values()) {
            if (!rebalanceFiles.contains(file.filename)) {
                index.remove(file.filename);
            } else if (file.lifecycle.equals("remove in progress")) {
                for (DstoreObject dstore : file.dStore.values()) {
                    PrintWriter out = new PrintWriter(dstore.socket.getOutputStream(), true);
                    out.println("REMOVE " + file.filename);
                }
            }
        } 

        for (Index file : index.values()) {
            if (file.dStore.size() < repFactor) {
                DstoreObject ds = file.dStore.get(file.dStore.keySet().toArray()[0]);
                var targetDstores = new CopyOnWriteArrayList<DstoreObject>();
                for (int i = file.dStore.size(); i < repFactor; i++) {
                    var store = getDstoreForStore(file.filename, targetDstores, ds);
                    targetDstores.add(store);
                    //TODO: May need to move these to happen after REBALANCE_COMPLETE is received
                    dstoresFiles.get(store).add(file.filename);
                    file.dStore.put(store.port, store);
                }
                rebalance.add(new DstoreRebalance(ds, RebalanceOperation.SEND, targetDstores, file.filename));
            }
        }

        for (DstoreObject ds : getDstoresMax()) {
            DstoreObject dsMin = getDstoresMin().get(0);
            int diff = dstoresFiles.get(ds).size() - dstoresFiles.get(dsMin).size();
            while (diff >= 2) {
                var targetDstores = new CopyOnWriteArrayList<DstoreObject>();
                var d = getDstoreForStore(ds);
                Index f = null;
                for (Index file : index.values()) {
                    if (!file.dStore.contains(d)) {
                        f = file;
                        break;
                    }
                }
                if (d != null) {
                    targetDstores.add(d);
                } else {
                    continue;
                }
                rebalance.add(new DstoreRebalance(ds, RebalanceOperation.SEND, targetDstores, f.filename));
                f.dStore.put(d.port, d);
                f.dStore.remove(ds.port);
                dstoresFiles.get(d).add(f.filename);
                dstoresFiles.get(ds).remove(f.filename);
                rebalance.add(new DstoreRebalance(ds, RebalanceOperation.DELETE, null, f.filename));
                diff = dstoresFiles.get(ds).size() - dstoresFiles.get(dsMin).size();
                // targetDstores.clear();
            }
        }
        executeRebalance();
    }

    private static void startRebalance() throws Exception {
        while (pendingOp.get()) {
            //Nothing
        }
        PrintWriter out;
        dStoresReplied.set(0);
        rebalance.clear();
        isRebalancing.set(true);
        rebalanceLatch = new CountDownLatch(dstores.size());
        rebalanceCompleteLatch = new CountDownLatch(dstores.size());
        for (DstoreObject dStore : dstores.values()) {
            out = new PrintWriter(dStore.socket.getOutputStream(), true);
            out.println("LIST");
        }
        if (rebalanceLatch.await(timeout, TimeUnit.MILLISECONDS)) {
            System.out.println("PUTKA"); //TODO: Remove!
            rebalance();
        } else {
            //TODO: throw some error
        }
    }

    private static CopyOnWriteArrayList<DstoreRebalance> getByDstore(DstoreObject ds) {
        var result = new CopyOnWriteArrayList<DstoreRebalance>();
        for (DstoreRebalance command : rebalance) {
            if (ds.equals(command.initialDstore)) {
                result.add(command);
            }
        }
        return result;
    }

    private static CopyOnWriteArrayList<DstoreObject> getDstoresMax() {
        CopyOnWriteArrayList<DstoreObject> returnValue = new CopyOnWriteArrayList<DstoreObject>();
        Map<DstoreObject, CopyOnWriteArrayList<String>> sorted = dstoresFiles.entrySet().stream()
                .sorted(comparingInt(e -> -e.getValue().size()))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> {
                            throw new AssertionError();
                        },
                        LinkedHashMap::new));
        for (DstoreObject dstore : sorted.keySet()) {
            returnValue.add(dstore);
        }
        return returnValue;
    }

    private static CopyOnWriteArrayList<DstoreObject> getDstoresMin() {
        CopyOnWriteArrayList<DstoreObject> returnValue = new CopyOnWriteArrayList<DstoreObject>();
        Map<DstoreObject, CopyOnWriteArrayList<String>> sorted = dstoresFiles.entrySet().stream()
                .sorted(comparingInt(e -> e.getValue().size()))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> {
                            throw new AssertionError();
                        },
                        LinkedHashMap::new));
        for (DstoreObject dstore : sorted.keySet()) {
            returnValue.add(dstore);
        }
        return returnValue;
    }

    private static DstoreObject getDstoreForStore(String filename) {
        DstoreObject returnValue = null;
        Map<DstoreObject, CopyOnWriteArrayList<String>> sorted = dstoresFiles.entrySet().stream()
                .sorted(comparingInt(e -> e.getValue().size()))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> {
                            throw new AssertionError();
                        },
                        LinkedHashMap::new));
        for (DstoreObject dstore : sorted.keySet()) {
            if (!dstoresFiles.get(dstore).contains(filename)) {
                returnValue = dstore;
                return returnValue;
            }
        }
        return returnValue;
    }

    private static DstoreObject getDstoreForStore(String filename, CopyOnWriteArrayList<DstoreObject> ds, DstoreObject dObj) {
        DstoreObject returnValue = null;
        Map<DstoreObject, CopyOnWriteArrayList<String>> sorted = dstoresFiles.entrySet().stream()
                .sorted(comparingInt(e -> e.getValue().size()))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> {
                            throw new AssertionError();
                        },
                        LinkedHashMap::new));
        for (DstoreObject dstore : sorted.keySet()) {
            if (!dstoresFiles.get(dstore).contains(filename) && !ds.contains(dstore) && !dstore.equals(dObj)) {
                returnValue = dstore;
                return returnValue;
            }
        }
        return returnValue;
    }

    private static DstoreObject getDstoreForStore(DstoreObject dObj) {
        DstoreObject returnValue = null;
        Map<DstoreObject, CopyOnWriteArrayList<String>> sorted = dstoresFiles.entrySet().stream()
                .sorted(comparingInt(e -> e.getValue().size()))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> {
                            throw new AssertionError();
                        },
                        LinkedHashMap::new));
        for (DstoreObject dstore : sorted.keySet()) {
            if (!dstore.equals(dObj)) {
                returnValue = dstore;
                return returnValue;
            }
        }
        return returnValue;
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

class DstoreRebalance {
    public DstoreObject initialDstore;
    public CopyOnWriteArrayList<DstoreObject> targetDstores;
    public RebalanceOperation operation;
    public String file;

    public DstoreRebalance (DstoreObject initialDstore, RebalanceOperation operation, CopyOnWriteArrayList<DstoreObject> targetDstores, String file) {
        this.initialDstore = initialDstore;
        this.operation = operation;
        this.targetDstores = targetDstores;
        this.file = file;
    }
}

enum RebalanceOperation {
    DELETE,
    SEND
}