import java.io.*;
import java.net.*;

public class Dstore {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[1]);
        int cport = Integer.parseInt(args[0]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];
        System.out.println("port: " + port + ", " + "controller port: " +
                cport + ", " + "timeout: " + timeout + ", " + "files folder: " + file_folder);

        Socket socket = null;
        ServerSocket ss = null;
        try {
            socket = new Socket(InetAddress.getLocalHost(), cport);
            ss = new ServerSocket(port);

            PrintWriter out = new PrintWriter(socket.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
