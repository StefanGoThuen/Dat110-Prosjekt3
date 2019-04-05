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

public class NodeClientWriter extends Thread {

	private boolean succeed = false;
	private String content;
	private String filename;
	
	public NodeClientWriter(String content, String filename) {
		this.content = content;
		this.filename = filename;
	}
	
	public void run() {
		sendRequest();
	}
	
	private void sendRequest() {
		String activeNode = StaticTracker.ACTIVENODES[0];
		BigInteger hashNode = Hash.hashOf(activeNode);
		try {
			ChordNodeInterface node = (ChordNodeInterface) Util.locateRegistry(activeNode).lookup(hashNode.toString());
			FileManager fm = new FileManager(node, StaticTracker.N);
			succeed = fm.requestWriteToFileFromAnyActiveNode(filename, content);
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

	public void setSucceed(boolean succeed) {
		this.succeed = succeed;
	}

}
