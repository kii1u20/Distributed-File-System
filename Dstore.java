import java.io.*;
import java.net.*;

public class Dstore {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];
        System.out.println("port: " + port + ", " + "controller port: " +
                cport + ", " + "timeout: " + timeout + ", " + "files folder: " + file_folder);

        Socket server = null;
        ServerSocket ss = null;
        try {
            server = new Socket(InetAddress.getLocalHost(), cport);
            ss = new ServerSocket(port);

            File folder = new File(file_folder);
            if (!folder.exists()) {
                folder.mkdir(); //TODO: if false display "ERROR cannot create Dstore directory"
            }

            PrintWriter out = new PrintWriter(server.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(server.getInputStream()));
            
            out.println("JOIN " + port);

            // final Socket client = ss.accept();

            String line;
            while (true) {
                while ((line = in.readLine()) != null) {
                    System.out.println("Received from controller: " + line);
                    if (line.equals("LIST")) {
                        String filenames = getFileNames(folder);
                        out.println("LIST " + filenames);
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
}
