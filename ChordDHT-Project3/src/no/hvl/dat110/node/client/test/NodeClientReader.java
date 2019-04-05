package no.hvl.dat110.node.client.test;

import java.math.BigInteger;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.List;

import no.hvl.dat110.file.FileManager;
import no.hvl.dat110.node.Message;
import no.hvl.dat110.rpc.StaticTracker;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

public class NodeClientReader extends Thread {

	private boolean succeed = false;
	
	private String filename;
	
	public NodeClientReader(String filename) {
		this.filename = filename;
	}
	
	public void run() {
		sendRequest();
	}
	
	private void sendRequest() {
		
		String activeNode = StaticTracker.ACTIVENODES[0];
		// Compute the hash of the node's IP address
		BigInteger hashNode = Hash.hashOf(activeNode);

		// use the hash to retrieve the ChordNodeInterface remote object from the registry
		try {
			ChordNodeInterface node = (ChordNodeInterface) Util.locateRegistry(activeNode).lookup(hashNode.toString());
			// do: FileManager fm = new FileManager(ChordNodeInterface, StaticTracker.N);
			FileManager fm = new FileManager(node, StaticTracker.N);
			// do: boolean succeed = fm.requestWriteToFileFromAnyActiveNode(filename, content);
			succeed = fm.requestToReadFileFromAnyActiveNode(filename);
		} catch (RemoteException e) {
			e.printStackTrace();
			succeed = false;
		} catch (NotBoundException e) {
			e.printStackTrace();
			succeed = false;
		}
	
	}
	
	public boolean isSucceed() {
		return succeed;
	}

}
