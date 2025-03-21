import java.io.*;
import java.net.*;
import java.util.*;


public class Coordinator {
    private int port;
    private int timeout;
    private ServerSocket serverSocket;
    private final Map<Integer, Socket> participantMap = new HashMap<>();
    private final List<Integer> activeParticipants = new ArrayList<>();
    private final Map<Long, String> messageMap = new TreeMap<>();
    private final Map<Long, List<Integer>> nonMessageRecipients = new TreeMap<>();

    public Coordinator(int port, int timeout) {
        this.port = port;
        this.timeout = timeout;
    }


    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Coordinator started on port: " + port);

            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new WorkerThread(socket, timeout)).start();
            }
        } catch (IOException e) {
            System.err.println("Coordinator Error: " + e.getMessage());
        }
    }


    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java Coordinator <config_file>");
            return;
        }

        try (Scanner scanner = new Scanner(new File(args[0]))) {
            int port = Integer.parseInt(scanner.nextLine().trim());
            int timeout = Integer.parseInt(scanner.nextLine().trim());
            new Coordinator(port, timeout).start();
        } catch (FileNotFoundException e) {
            System.err.println("Configuration file not found: " + args[0]);
        }
    }


    private class WorkerThread implements Runnable {
        private Socket socket;
        private int timeout;
        private DataInputStream in;
        private DataOutputStream out;

        public WorkerThread(Socket socket, int timeout) {
            this.socket = socket;
            this.timeout = timeout;
            try {
                in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                out = new DataOutputStream(socket.getOutputStream());
            } catch (IOException e) {
                System.err.println("WorkerThread IO Error: " + e.getMessage());
            }
        }


        @Override
        public void run() {
            try {
                String input = in.readUTF();
                System.out.println("Received: " + input);
                handleRequest(input);
            } catch (IOException e) {
                System.err.println("WorkerThread Connection Error: " + (e.getMessage() != null ? e.getMessage() : "No message"));
                try {
                    out.writeUTF("Error processing request: " + (e.getMessage() != null ? e.getMessage() : "Unknown error"));
                    out.flush();
                } catch (IOException ignored) {}
            } finally {
                try {
                    Thread.sleep(100);
                    socket.close();
                } catch (InterruptedException | IOException e) {
                    System.err.println("Error closing socket: " + e.getMessage());
                }
            }
        }


        private void handleRequest(String input) throws IOException {
            String[] parts = input.split("#");
            String command = parts[0];
            int participantID = Integer.parseInt(parts[1]);

            switch (command) {
                case "register":
                    registerParticipant(participantID, parts[2], Integer.parseInt(parts[3]));
                    break;
                case "deregister":
                    deregisterParticipant(participantID);
                    break;
                case "reconnect":
                    reconnectParticipant(participantID, parts[2], Integer.parseInt(parts[3]));
                    break;
                case "disconnect":
                    disconnectParticipant(participantID);
                    break;
                case "msend":
                    multicastMessage(parts[2], participantID);
                    break;
                default:
                    out.writeUTF("Invalid command");
                    out.flush();
                    break;
            }
        }



        private void registerParticipant(int id, String ip, int port) throws IOException {
            String response;
            if (participantMap.containsKey(id)) {
                response = "Participant already registered";
            } else {
                try {
                    Socket participantSocket = new Socket(ip, port);
                    participantMap.put(id, participantSocket);
                    activeParticipants.add(id);
                    response = "Participant registered";
                } catch (IOException e) {
                    System.err.println("Failed to connect to participant " + id + " at " + ip + ":" + port + ": " + e.getMessage());
                    response = "Registration failed: " + e.getMessage();
                }
            }
            out.writeUTF(response);
            out.flush();
        }



        private void deregisterParticipant(int id) throws IOException {
            String response;
            if (participantMap.containsKey(id)) {
                if (participantMap.get(id) != null) {
                    participantMap.get(id).close();
                    activeParticipants.remove((Integer) id);
                }
                participantMap.remove(id);
                response = "Participant deregistered";
            } else {
                response = "Participant not found";
            }
            out.writeUTF(response);
            out.flush();
        }




        private void reconnectParticipant(int id, String ip, int port) throws IOException {
            String response;
            if (participantMap.containsKey(id) && !activeParticipants.contains(id)) {
                Socket participantSocket = new Socket(ip, port);
                participantMap.put(id, participantSocket);
                activeParticipants.add(id);
                response = "Participant reconnected";
                resendPendingMessages(id, participantSocket);
            } else {
                response = "Participant is already connected or not found";
            }
            out.writeUTF(response);
            out.flush();
        }



        private void disconnectParticipant(int id) throws IOException {
            String response;
            if (participantMap.containsKey(id)) {
                if (participantMap.get(id) != null) {
                    participantMap.get(id).close();
                }
                participantMap.put(id, null);
                activeParticipants.remove((Integer) id);
                response = "Participant disconnected";
            } else {
                response = "Participant not found";
            }
            out.writeUTF(response);
            out.flush();
        }



        private void multicastMessage(String message, int senderID) throws IOException {
            boolean allSent = true;
            List<Integer> failedParticipants = new ArrayList<>();
            for (Integer participantID : activeParticipants) {
                Socket socket = participantMap.get(participantID);
                if (socket != null && !socket.isClosed()) {
                    try {
                        DataOutputStream participantOut = new DataOutputStream(socket.getOutputStream());
                        participantOut.writeUTF(message);
                        participantOut.writeUTF("eof");
                        participantOut.flush();
                    } catch (IOException e) {
                        System.err.println("Failed to send to participant " + participantID + ": " + e.getMessage());
                        failedParticipants.add(participantID);
                        allSent = false;
                    }
                } else {
                    allSent = false;
                }
            }
            for (Integer id : failedParticipants) {
                if (participantMap.get(id) != null) {
                    participantMap.get(id).close();
                }
                participantMap.put(id, null);
                activeParticipants.remove(id);
            }
            if (allSent) {
                out.writeUTF("Message Acknowledged");
            } else {
                out.writeUTF("Message delivery failed to some participants");
            }
            saveMessageForOfflineParticipants(message, senderID);
            out.flush();
        }



        private void saveMessageForOfflineParticipants(String message, int senderID) {
            long timestamp = System.currentTimeMillis();
            messageMap.put(timestamp, message);
            List<Integer> nonRecipients = new ArrayList<>();
            for (Integer participantID : participantMap.keySet()) {
                if (!activeParticipants.contains(participantID)) {
                    nonRecipients.add(participantID);
                }
            }
            nonMessageRecipients.put(timestamp, nonRecipients);
        }



        private void resendPendingMessages(int participantID, Socket socket) throws IOException {
            DataOutputStream resendOut = new DataOutputStream(socket.getOutputStream());
            Iterator<Long> iterator = messageMap.keySet().iterator();
            while (iterator.hasNext()) {
                long messageTime = iterator.next();
                if (System.currentTimeMillis() - messageTime > timeout * 1000) {
                    iterator.remove();
                    nonMessageRecipients.remove(messageTime);
                } else if (nonMessageRecipients.getOrDefault(messageTime, new ArrayList<>()).contains(participantID)) {
                    resendOut.writeUTF(messageMap.get(messageTime));
                    resendOut.writeUTF("eof");
                    resendOut.flush();
                    nonMessageRecipients.get(messageTime).remove((Integer) participantID);
                    if (nonMessageRecipients.get(messageTime).isEmpty()) {
                        iterator.remove();
                        nonMessageRecipients.remove(messageTime);
                    }
                }
            }
        }
    }
}