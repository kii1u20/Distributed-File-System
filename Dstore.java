import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

//TODO: When the controller stops, Dstores should stop as well
public class Dstore {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];
        System.out.println("port: " + port + ", " + "controller port: " +
                cport + ", " + "timeout: " + timeout + ", " + "files folder: " + file_folder);

        Socket server = null;
        CopyOnWriteArrayList<String> rebalanceFile = new CopyOnWriteArrayList<String>();
        // ServerSocket ss = null;
        try {
            server = new Socket(InetAddress.getLocalHost(), cport);
            ServerSocket ss = new ServerSocket(port);

            File folder = new File(file_folder);
            if (!folder.exists()) {
                folder.mkdir(); // TODO: if false display "ERROR cannot create Dstore directory"
            }
            emptyFolder(folder);

            PrintWriter out = new PrintWriter(server.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(server.getInputStream()));

            out.println("JOIN " + port);

            new Thread(new Runnable() {
                public void run() {
                    try {
                        while (true) {
                            final Socket client = ss.accept();
                            new Thread(new Runnable() {
                                public void run() {
                                    try {
                                        InputStream input = client.getInputStream();
                                        OutputStream output = client.getOutputStream();
                                        BufferedReader inCl = new BufferedReader(
                                                new InputStreamReader(input));
                                        PrintWriter outCl = new PrintWriter(output, true);

                                        String line;
                                        while ((line = inCl.readLine()) != null) {
                                            System.out.println("Command: " + line);
                                            if (line.contains("REBALANCE_STORE ")) {
                                                String filename = line.split(" ")[1];
                                                String filesize = line.split(" ")[2];
                                                rebalanceFile.clear();
                                                rebalanceFile.add(filename);
                                                outCl.println("ACK"); //DOESNT REGISTER FIX
                                                File outputFile = new File(file_folder, filename);
                                                FileOutputStream fOut = new FileOutputStream(outputFile);
                                                byte[] buffer = new byte[1024];
                                                while ((buffer = input.readNBytes(1024)).length != 0) {
                                                    System.out.println("*");
                                                    fOut.write(buffer);
                                                }
                                                fOut.close();
                                                // input.close();
                                                // client.close();
                                            } else if (line.contains("LOAD_DATA ")) {
                                                String filename = line.split(" ")[1];
                                                File file = new File(file_folder, filename);
                                                if (!file.exists()) {
                                                    client.close();
                                                    break;
                                                }
                                                FileInputStream fIn = new FileInputStream(file);
                                                byte[] buffer = new byte[1024];
                                                int len;
                                                while ((len = fIn.read(buffer)) != -1) {
                                                    System.out.println("*");
                                                    output.write(buffer, 0, len);
                                                }
                                            } else if (line.contains("STORE ")) {
                                                String[] attr = line.split(" ");
                                                String filename = attr[1];
                                                String filesize = attr[2]; //TODO: Figure out what this is for
                                                                           //Maybe to specify the size of the byte buffer?
                                                outCl.println("ACK");
                                                File outputFile = new File(file_folder, filename);
                                                FileOutputStream fOut = new FileOutputStream(outputFile);
                                                byte[] buffer = new byte[1024];
                                                while ((buffer = input.readNBytes(1024)).length != 0) {
                                                    System.out.println("*");
                                                    fOut.write(buffer);
                                                }
                                                out.println("STORE_ACK " + filename);
                                                fOut.close();
                                            }
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
            }).start();

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("Received from controller: " + line);
                if (line.equals("LIST")) {
                    String filenames = getFileNames(folder);
                    out.println("LIST " + filenames);
                } else if (line.contains("REMOVE ")) {
                    String filename = line.split(" ")[1];
                    File file = new File(file_folder, filename);
                    if (file.delete()) {
                        out.println("REMOVE_ACK " + filename);
                    } else {
                        out.println("ERROR_FILE_DOES_NOT_EXIST " + filename);
                    }
                } else if (line.contains("REBALANCE ")) {
                    //REBALANCE 5 test1.txt 1 54321 test5.txt 1 54321 test2.txt 1 54321 test3.txt 1 54321 test4.txt 1 54321
                    line = line.replace("REBALANCE ", "");
                    String[] split = line.split(" ");
                    ArrayList<String> sp = new ArrayList<String>(Arrays.asList(split));
                    sp.remove(0);
                    ConcurrentHashMap<String, Integer> files = new ConcurrentHashMap<String, Integer>();
                    CopyOnWriteArrayList<Integer> dstores = new CopyOnWriteArrayList<Integer>();
                    var iterator = sp.iterator();
                    while (iterator.hasNext()) {
                        String next = iterator.next();
                        if (next.contains(".")) {
                            files.put(next, Integer.parseInt(iterator.next()));
                        } else if (next.length() >= 4) {
                            dstores.add(Integer.parseInt(next));
                        }
                    }
                    var dstoresIterator = dstores.iterator();
                    for (String file : files.keySet()) {
                        int filesize = 5; //TODO: get the size of the file
                        int numberOfDs = files.get(file);
                        for (int i = 0; i < numberOfDs; i++) {
                            int nextPort = dstoresIterator.next();
                            Socket ds = new Socket(InetAddress.getLocalHost(), nextPort);
                            PrintWriter dsOut = new PrintWriter(ds.getOutputStream(), true);
                            BufferedReader inCl = new BufferedReader(new InputStreamReader(ds.getInputStream()));
                            String ack;
                            dsOut.println("REBALANCE_STORE " + file + " " + filesize);
                            while ((ack = inCl.readLine()) != null) {
                                if (ack.equals("ACK")) {
                                    System.out.println("Reballance ACK received");
                                    try {
                                        File fileR = new File(file_folder, file);
                                        if (!fileR.exists()) {
                                            ds.close();
                                            System.err.println("The file doesn't exist");
                                            break;
                                        }
                                        FileInputStream fIn = new FileInputStream(fileR);
                                        // byte[] buffer = fIn.readAllBytes();
                                        // ds.getOutputStream().write(buffer);
                                        byte[] buffer = new byte[1024];
                                        int len;
                                        while ((len = fIn.read(buffer)) != -1) {
                                            System.out.println("*");
                                            ds.getOutputStream().write(buffer, 0, len);
                                        }
                                        fIn.close();
                                        dsOut.close();
                                        inCl.close();
                                        ds.close();
                                        out.println("REBALANCE_COMPLETE");
                                        break;
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getFileNames(File folder) {
        File[] listOfFiles = folder.listFiles();
        String result = "";
        for (File file : listOfFiles) {
            result += file.getName() + " ";
        }
        return result.stripTrailing();
    }

    private static void emptyFolder(File folder) {
        File[] files = folder.listFiles();
        for (File file : files) {
            file.delete();
        }
    }
}
