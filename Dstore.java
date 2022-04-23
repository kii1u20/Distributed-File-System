import java.io.*;
import java.net.*;

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
                                            if (line.contains("STORE ")) {
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
                                            } else if (line.contains("LOAD_DATA ")) {
                                                String filename = line.split(" ")[1];
                                                File file = new File(file_folder, filename);
                                                if (!file.exists()) {
                                                    client.close();
                                                    break;
                                                }
                                                FileInputStream fIn = new FileInputStream(file);
                                                byte[] buffer = new byte[1024];
                                                while (fIn.read(buffer) != -1) {
                                                    System.out.println("*");
                                                    output.write(buffer);
                                                }
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
