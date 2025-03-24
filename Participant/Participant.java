import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Participant {
    private int uniqueID;
    private String coordinatorIP;
    private int coordinatorPort;
    private BufferedReader userInput;
    private String logFile;
    private ReceiverThread receiverThread;

    public Participant(int id, String logFile, String address, int port) {
        this.uniqueID = id;
        this.logFile = logFile;
        this.coordinatorIP = address;
        this.coordinatorPort = port;
        startConnection();
    }

    private void startConnection() {
        userInput = new BufferedReader(new InputStreamReader(System.in));
        new Thread(this::handleUserCommands, "Thread-A").start();
    }

    private void handleUserCommands() {
        try {
            String command;
            while (true) {
                System.out.print("input> ");
                command = userInput.readLine();
                if (command == null || command.equalsIgnoreCase("quit")) {
                    if (receiverThread != null) {
                        receiverThread.stopThread();
                    }
                    break;
                }
                processCommand(command);
            }
        } catch (IOException e) {
            System.err.println("Error reading user input: " + e.getMessage());
        } finally {
            try {
                userInput.close();
            } catch (IOException e) {
                System.err.println("Error closing user input: " + e.getMessage());
            }
        }
    }

    private void processCommand(String command) {
        String[] parts = command.split(" ", 2);
        String action = parts[0];
        String response = "";

        Socket socket = null;
        DataOutputStream coordinatorOutput = null;
        DataInputStream coordinatorInput = null;

        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(coordinatorIP, coordinatorPort), 5000);
            socket.setSoTimeout(5000);
            coordinatorOutput = new DataOutputStream(socket.getOutputStream());
            coordinatorInput = new DataInputStream(socket.getInputStream());

            switch (action) {
                case "register":
                    if (parts.length < 2) {
                        System.out.println("Usage: register [port]");
                        return;
                    }
                    int listenPort = Integer.parseInt(parts[1]);
                    ReceiverThread newReceiverThread = new ReceiverThread(logFile, listenPort);
                    Thread.sleep(100); // Allow thread to attempt binding
                    if (!newReceiverThread.isListening()) {
                        System.out.println("Failed to bind to port " + listenPort);
                        newReceiverThread.stopThread();
                        return;
                    }
                    coordinatorOutput.writeUTF("register#" + uniqueID + "#" + InetAddress.getLocalHost().getHostAddress() + "#" + listenPort);
                    coordinatorOutput.flush();
                    response = coordinatorInput.readUTF();
                    if (response.equals("Participant registered")) {
                        System.out.println("Listening on port " + listenPort);
                        if (receiverThread != null) {
                            receiverThread.stopThread();
                        }
                        receiverThread = newReceiverThread;
                    } else {
                        newReceiverThread.stopThread();
                        System.out.println(response);
                    }
                    break;

                case "deregister":
                    coordinatorOutput.writeUTF("deregister#" + uniqueID);
                    coordinatorOutput.flush();
                    response = coordinatorInput.readUTF();
                    if (receiverThread != null && response.equals("Participant deregistered")) {
                        receiverThread.stopThread();
                        receiverThread = null;
                    }
                    System.out.println(response);
                    break;

                case "disconnect":
                    if (receiverThread != null) {
                        receiverThread.setShuttingDown(); // Signal shutdown before sending
                    }
                    coordinatorOutput.writeUTF("disconnect#" + uniqueID);
                    coordinatorOutput.flush();
                    response = coordinatorInput.readUTF();
                    if (receiverThread != null && response.equals("Participant disconnected")) {
                        receiverThread.stopThread();
                        receiverThread = null;
                    }
                    System.out.println(response);
                    break;

                case "reconnect":
                    if (parts.length < 2) {
                        System.out.println("Usage: reconnect [port]");
                        return;
                    }
                    listenPort = Integer.parseInt(parts[1]);
                    newReceiverThread = new ReceiverThread(logFile, listenPort);
                    Thread.sleep(100); // Allow thread to attempt binding
                    if (!newReceiverThread.isListening()) {
                        System.out.println("Failed to bind to port " + listenPort);
                        newReceiverThread.stopThread();
                        return;
                    }
                    coordinatorOutput.writeUTF("reconnect#" + uniqueID + "#" + InetAddress.getLocalHost().getHostAddress() + "#" + listenPort);
                    coordinatorOutput.flush();
                    response = coordinatorInput.readUTF();
                    if (response.equals("Participant reconnected")) {
                        System.out.println("Listening on port " + listenPort);
                        if (receiverThread != null) {
                            receiverThread.stopThread();
                        }
                        receiverThread = newReceiverThread;
                    } else {
                        newReceiverThread.stopThread();
                        System.out.println(response);
                    }
                    break;

                case "msend":
                    if (parts.length < 2) {
                        System.out.println("Usage: msend [message]");
                        return;
                    }
                    String message = parts[1];
                    coordinatorOutput.writeUTF("msend#" + uniqueID + "#" + message);
                    coordinatorOutput.flush();
                    response = coordinatorInput.readUTF();
                    System.out.println(response);
                    break;

                default:
                    response = "Invalid command";
                    System.out.println(response);
                    break;
            }
        } catch (SocketTimeoutException e) {
            System.err.println("Command timed out: " + e.getMessage());
            System.out.println("Command timed out");
        } catch (IOException e) {
            System.err.println("IO error in processCommand: " + e.getMessage());
            System.out.println("IO error: " + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println("Interrupted while processing command: " + e.getMessage());
            System.out.println("Interrupted");
        } finally {
            try {
                if (coordinatorInput != null) coordinatorInput.close();
                if (coordinatorOutput != null) coordinatorOutput.close();
                if (socket != null && !socket.isClosed()) socket.close();
            } catch (IOException e) {
                System.err.println("Error closing resources: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java Participant <config_file>");
            return;
        }

        try (Scanner scanner = new Scanner(new File(args[0]))) {
            int uniqueID = Integer.parseInt(scanner.nextLine());
            String logFile = scanner.nextLine();
            String[] coordinatorInfo = scanner.nextLine().split(" ");
            new Participant(uniqueID, logFile, coordinatorInfo[0], Integer.parseInt(coordinatorInfo[1]));
        } catch (FileNotFoundException e) {
            System.err.println("Configuration file not found: " + args[0]);
        }
    }
}

class ReceiverThread implements Runnable {
    private ServerSocket serverSocket;
    private int port;
    private String fileName;
    private volatile boolean running = true;
    private volatile boolean isShuttingDown = false;
    private volatile boolean isListening = false; // Track binding success
    private Socket clientSocket;
    private DataInputStream input;
    private FileOutputStream fileOut;

    ReceiverThread(String fileName, int port) {
        this.fileName = fileName;
        this.port = port;
        new Thread(this, "Thread-B").start();
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            isListening = true; // Binding succeeded
            while (running) {
                try {
                    clientSocket = serverSocket.accept();
                    input = new DataInputStream(clientSocket.getInputStream());
                    fileOut = new FileOutputStream(fileName, true);

                    while (running) {
                        String message = input.readUTF();
                        if (message.equals("eof")) continue;
                        System.out.println("Received: " + message);
                        fileOut.write((message + "\n").getBytes());
                        fileOut.flush();
                    }
                } catch (IOException e) {
                    if (running && !isShuttingDown) {
                        System.err.println("Receiver error during message read: " + (e.getMessage() != null ? e.getMessage() : "No message"));
                    }
                } finally {
                    try {
                        if (fileOut != null) fileOut.close();
                        if (input != null) input.close();
                        if (clientSocket != null && !clientSocket.isClosed()) clientSocket.close();
                    } catch (IOException e) {
                        System.err.println("Error closing temporary resources: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            if (running && !isShuttingDown) {
                System.err.println("Receiver error on startup: " + e.getMessage());
            }
            isListening = false; // Binding failed
        }
    }

    public boolean isListening() {
        return isListening;
    }

    public void setShuttingDown() {
        isShuttingDown = true;
    }

    public void stopThread() {
        isShuttingDown = true;
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            if (clientSocket != null && !clientSocket.isClosed()) {
                clientSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing receiver: " + e.getMessage());
        }
    }
}