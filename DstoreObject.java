import java.net.Socket;

public class DstoreObject {
    Socket socket;
    int port;

    public DstoreObject (Socket socket, int port) {
        this.socket = socket;
        this.port = port;
    }
}
