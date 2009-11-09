package org.nordugrid.gridftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.globus.ftp.DataSinkStream;
import org.globus.ftp.DataSourceStream;
import org.globus.ftp.GridFTPClient;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.Marker;
import org.globus.ftp.MarkerListener;
import org.globus.ftp.MlsxEntry;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;

final class ARCGridFTPJobInterface {

	private static Logger log = Logger.getLogger(ARCGridFTPJobInterface.class);
	
	private GlobusURL url = null;

	protected GridFTPClient client = null;

	public ARCGridFTPJobInterface(String u) throws ARCGridFTPJobException {
		try {
			url = new GlobusURL(u);
		}
		catch (MalformedURLException err) {
			throw new ARCGridFTPJobException("Bad URL " + u + ": " + err.getMessage());
		}
		if (!url.getProtocol().equals("gsiftp")) {
			String err = new String("Protocol " + url.getProtocol() + " not supported for job submission");
			url = null;
			throw new ARCGridFTPJobException(err);
		}
	}
	
	public GlobusURL getUrl() {
		return url;
	}
	public void setUrl(GlobusURL url) {
		this.url = url;
	}
	protected void finalize() {
		disconnect();
	}

	public void connect() throws ARCGridFTPJobException {
		connect(null, null);
	}

	public void connect(String proxylocation) throws ARCGridFTPJobException {
		connect(proxylocation, null);
	}

	public void connect(GlobusCredential gc) throws ARCGridFTPJobException {
		connect(null, gc);
	}

	public void connect(String proxylocation, GlobusCredential gc) throws ARCGridFTPJobException {
		if (client == null) {
			if (url == null) {
				throw new ARCGridFTPJobException("URL not defined");
			}
			try {
				int port = url.getPort();
				if (port == -1) {
					port = GlobusURL.getPort(url.getProtocol());
				}
				client = new GridFTPClient(url.getHost(), port);
				GSSManager manager = ExtendedGSSManager.getInstance();
				GlobusCredential gcred = gc;
				if (proxylocation != null) {
					try {
						gcred = new GlobusCredential(proxylocation);
					}
					catch (Exception e) {
						System.err.println("reading proxy from file failed, continuing");
					}
				}
				GSSCredential cred = null;
				if (gcred != null) {
					GlobusGSSCredentialImpl globusgsscred = new GlobusGSSCredentialImpl(gcred, 0);
					cred = globusgsscred;
				}
				else {
					cred = manager.createCredential(GSSCredential.INITIATE_ONLY);
				}
				client.authenticate(cred);
				client.changeDir("/" + url.getPath());
			}
			catch (IOException err) {
				client = null;
				// err.printStackTrace();
				err.printStackTrace();
				throw new ARCGridFTPJobException("IO error: "+err.getMessage());
			}
			catch (ServerException err) {
				if (log.isDebugEnabled()) {
					err.printStackTrace();
				}
				client = null;
				throw new ARCGridFTPJobException("Communication with server failed: " + err.getMessage());
			}
			catch (GSSException err) {
				err.printStackTrace();
				client = null;
				throw new ARCGridFTPJobException("Failed to use credentials: " + err.getMessage());
			}
		}
	}

	public void disconnect() {
		if (client != null) {
			try {
				client.close();
			}
			catch (ServerException err) {
				client = null;
			}
			catch (IOException err) {
				client = null;
			}
			client = null;
		}
	}

	// submit using default proxy
	public String submit() throws ARCGridFTPJobException {
		return submit(null, null);
	}

	// submit using a proxy location e.g. /tmp/proxy
	public String submit(String proxylocation) throws ARCGridFTPJobException {
		return submit(proxylocation, null);
	}

	// submit using a credential object
	public String submit(GlobusCredential proxy) throws ARCGridFTPJobException {
		return submit(null, proxy);
	}

	private String submit(String proxyloc, GlobusCredential proxy) throws ARCGridFTPJobException {
		if (client == null) {
			if (proxyloc != null)
				connect(proxyloc);
			else if (proxy != null)
				connect(proxy);
			else
				connect();
		}
		String id = new String("");
		try {
			client.changeDir("new");
			id = client.getCurrentDir();
			int p = id.lastIndexOf('/');
			if (p != -1) {
				id = id.substring(p + 1);
			}
		}
		catch (IOException err) {
			disconnect();
			throw new ARCGridFTPJobException("IO error: " + err.getMessage());
		}
		catch (ServerException err) {
			disconnect();
			throw new ARCGridFTPJobException("Communication with server failed: " + err.getMessage());
		}
		return id;
	}

	public void cancel(String id) throws ARCGridFTPJobException {
		if (client == null)
			connect();
		try {
			client.deleteFile("/" + url.getPath() + "/" + id);
		}
		catch (IOException err) {
			disconnect();
			throw new ARCGridFTPJobException("IO error: " + err.getMessage());
		}
		catch (ServerException err) {
			disconnect();
			throw new ARCGridFTPJobException("Communication with server failed: " + err.getMessage());
		}
	}

	public void clean(String id) throws ARCGridFTPJobException {
		if (client == null)
			connect();
		try {
			client.deleteDir("/" + url.getPath() + "/" + id);
		}
		catch (IOException err) {
			disconnect();
			throw new ARCGridFTPJobException("IO error: " + err.getMessage());
		}
		catch (ServerException err) {
			disconnect();
			throw new ARCGridFTPJobException("Communication with server failed: " + err.getMessage());
		}
	}

	public void upload(InputStream in, String name) throws ARCGridFTPJobException {
		if (client == null)
			connect();
		try {
			client.setPassiveMode(true);
			 // Fix by Frederik Orellana.
		      // Without this, text files transferred from Windows, although with
		      // UNIX newlines, will end up with Windows newlines.
		    client.setType(GridFTPSession.TYPE_IMAGE);
			DataSourceStream stream = new DataSourceStream(in);
			client.put("/" + url.getPath() + "/" + name, stream, new DummyMarkerListener());
		}
		catch (IOException err) {
			disconnect();
			throw new ARCGridFTPJobException("IO error: " + err.getMessage());
		}
		catch (ServerException err) {
			disconnect();
			throw new ARCGridFTPJobException("Communication with server failed: " + err.getMessage());
		}
		catch (ClientException err) {
			disconnect();
			throw new ARCGridFTPJobException("Local problems: " + err.getMessage());
		}
	}

	public void download(OutputStream out, String name) throws ARCGridFTPJobException {
		if (client == null)
			connect();
		try {
			client.setPassiveMode(true);
			DataSinkStream stream = new DataSinkStream(out);
			DummyMarkerListener mark = new DummyMarkerListener();
			client.get("/" + url.getPath() + "/" + name, stream, mark);
		}
		catch (IOException err) {
			disconnect();
			throw new ARCGridFTPJobException("IO error: " + err.getMessage());
		}
		catch (ServerException err) {
			err.printStackTrace();
			disconnect();
			throw new ARCGridFTPJobException("Communication with server '" + url.getHost() + "' failed while trying to download " + url.getPath()
					+ "/" + name + ": " + err.getMessage());
		}
		catch (ClientException err) {
			disconnect();
			throw new ARCGridFTPJobException("Local problems: " + err.getMessage());

		}
	}

	public void list(String dir, List dirs, List files) throws ARCGridFTPJobException {
		if (client == null)
			connect();
		Vector list = null;
		try {
			String fdir = new String("/" + url.getPath());
			if (!dir.equals(""))
				fdir += "/" + dir;
			client.setPassiveMode(true);
			list = client.mlsd(fdir);
			for (int i = 0; i < list.size(); i++) {
				MlsxEntry entry = (MlsxEntry) (list.get(i));
				String type = entry.get("type");
				String name = entry.getFileName();
				if (name.equals("."))
					continue;
				if (name.equals(".."))
					continue;
				if (!dir.equals(""))
					name = dir + "/" + name;
				if (type.equals("dir")) {
					dirs.add(name);
				}
				else if (type.equals("file")) {
					files.add(name);
				}
			}
		}
		catch (java.io.IOException err) {
			disconnect();
			throw new ARCGridFTPJobException("IO error: " + err.getMessage());
		}
		catch (ServerException err) {
			disconnect();
			if (err.getCode() != ServerException.SERVER_REFUSED) {
				throw new ARCGridFTPJobException("Communication with server failed: " + err.getMessage());
			}
			else {
				System.err.println("ERROR: " + err.toString());
			}
		}
		catch (ClientException err) {
			disconnect();
			throw new ARCGridFTPJobException("Local problems: " + err.getMessage());
		}

	}

	class DummyMarkerListener implements MarkerListener {
		public DummyMarkerListener() {
		}

		public void markerArrived(Marker m) {
		}
	}
}
