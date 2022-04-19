import java.net.Socket;

public class DstoreObject {
    Socket socket;
    int port;

    public DstoreObject (Socket soc, int p) {
        socket = soc;
        port = p;
    }
}
