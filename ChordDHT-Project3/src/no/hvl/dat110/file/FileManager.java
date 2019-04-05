package no.hvl.dat110.file;

/**
 * @author tdoy
 * dat110 - demo/exercise
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import no.hvl.dat110.node.Message;
import no.hvl.dat110.node.OperationType;
import no.hvl.dat110.node.Operations;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

public class FileManager extends Thread {

	private BigInteger[] replicafiles; // array stores replicated files for distribution to matching nodes
	private int nfiles = 4; // let's assume each node manages nfiles (5 for now) - can be changed from the
							// constructor
	private ChordNodeInterface node;

	public FileManager(ChordNodeInterface chordnode, int N) throws RemoteException {
		this.nfiles = N;
		replicafiles = new BigInteger[N];
		this.node = chordnode;
	}

	public void run() {

		while (true) {
			try {
				distributeReplicaFiles();
				Thread.sleep(3000);
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void createReplicaFiles(String filename) {

		for (int i = 0; i < nfiles; i++) {
			String replicafile = filename + i;
			replicafiles[i] = Hash.hashOf(replicafile);

		}
		// System.out.println("Generated replica file keyids for
		// "+chordnode.getNodeIP()+" => "+Arrays.asList(replicafiles));
	}

	public void distributeReplicaFiles() throws IOException {

		// lookup(keyid) operation for each replica
		// findSuccessor() function should be invoked to find the node with identifier
		// id >= keyid and store the file (create & write the file)

		for (int i = 0; i < replicafiles.length; i++) {
			BigInteger fileID = (BigInteger) replicafiles[i];
			ChordNodeInterface succFileID = node.findSuccessor(fileID);

			// if we find the successor node of fileID, we can assign the file to the
			// successor. This should always work even with one node
			if (succFileID != null) {
				succFileID.addToFileKey(fileID);
				String initialcontent = node.getNodeIP() + "\n" + node.getNodeID();
				succFileID.createFileInNodeLocalDirectory(initialcontent, fileID); // copy the file to the successor
																						// local dir
			}
		}
	}

	/**
	 * 
	 * @param filename
	 * @return list of active nodes in a list of messages having the replicas of
	 *         this file
	 * @throws RemoteException
	 */
	public Set<Message> requestActiveNodesForFile(String filename) throws RemoteException {
		Set<Message> activeNodes = new HashSet<>();
		createReplicaFiles(filename);
		for (int i = 0; i < replicafiles.length; i++) {
			BigInteger fileID = replicafiles[i];
			ChordNodeInterface successorFileID = node.findSuccessor(fileID);
			if (successorFileID != null) {
				Map<BigInteger, Message> succMap = successorFileID.getFilesMetadata();
				if (!checkDuplicateActiveNode(activeNodes, succMap.get(fileID))) {
					activeNodes.add(succMap.get(fileID));
				}
			}
		}

		return activeNodes;
	}

	private boolean checkDuplicateActiveNode(Set<Message> activenodesdata, Message nodetocheck) {

		for (Message nodedata : activenodesdata) {
			if (nodetocheck.getNodeID().compareTo(nodedata.getNodeID()) == 0)
				return true;
		}

		return false;
	}

	public boolean requestToReadFileFromAnyActiveNode(String filename) throws RemoteException, NotBoundException {

		Set<Message> activeNodesWithReplicas = requestActiveNodesForFile(filename);
		Message msg = new ArrayList<>(activeNodesWithReplicas).get(0);

		msg.setOptype(OperationType.READ);
		Registry registry = Util.locateRegistry(msg.getNodeIP());
		ChordNodeInterface node = (ChordNodeInterface) registry.lookup(msg.getNodeID().toString());
		node.setActiveNodesForFile(activeNodesWithReplicas);
		node.setActiveNodesForFile(activeNodesWithReplicas);
		msg.setNodeIP(node.getNodeIP());
		boolean result = node.requestReadOperation(msg);
		msg.setAcknowledged(result);
		node.multicastVotersDecision(msg);
		if (msg.isAcknowledged()) {
			node.acquireLock();
			Operations op = new Operations(node, msg, activeNodesWithReplicas);
			op.performOperation();
			op.multicastReadReleaseLocks();
			node.releaseLocks();
		}
		return msg.isAcknowledged();
	}

	public boolean requestWriteToFileFromAnyActiveNode(String filename, String newcontent)
			throws RemoteException, NotBoundException {

		// get all the activenodes that have the file (replicas) i.e.
		// requestActiveNodesForFile(String filename)
		Set<Message> activeNodes = requestActiveNodesForFile(filename);
		// choose any available node
		Message nodeMessage = new ArrayList<>(activeNodes).get(0);
		ChordNodeInterface node = (ChordNodeInterface) Util.locateRegistry(nodeMessage.getNodeIP())
				.lookup(nodeMessage.getNodeID().toString());
		Message message = new Message();
		message.setOptype(OperationType.READ);
		message.setNewcontent(newcontent);
		node.setActiveNodesForFile(activeNodes);
		message.setNodeIP(node.getNodeIP());
		boolean result = node.requestWriteOperation(message);
		message.setAcknowledged(result);
		node.multicastVotersDecision(message);
		if (message.isAcknowledged()) {
			node.acquireLock();
			node.incrementclock();
			Operations op = new Operations(node, message, activeNodes);
			op.performOperation();
			try {
				distributeReplicaFiles();
			} catch (IOException e) {
				e.printStackTrace();
			}
			op.multicastReadReleaseLocks();
			node.releaseLocks();
		}

		return message.isAcknowledged();

	}

	/**
	 * create the localfile with the node's name and id as content of the file
	 * 
	 * @param nodename
	 * @throws RemoteException
	 */
	public void createLocalFile() throws RemoteException {
		String nodename = node.getNodeIP();
		String path = new File(".").getAbsolutePath().replace(".", "");
		File fpath = new File(path + "/" + nodename); // we'll have ../../nodename/
		if (!fpath.exists()) {
			boolean suc = fpath.mkdir();
			try {
				if (suc) {
					File file = new File(fpath + "/" + nodename); // end up with: ../../nodename/nodename (actual file
																	// no ext)
					file.createNewFile();
					// write the node's data into this file
					writetofile(file);
				}
			} catch (IOException e) {

				// e.printStackTrace();
			}
		}

	}

	private void writetofile(File file) throws RemoteException {

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			bw.write(node.getNodeIP());
			bw.newLine();
			bw.write(node.getNodeID().toString());
			bw.close();

		} catch (IOException e) {

			// e.printStackTrace();
		}
	}
}
