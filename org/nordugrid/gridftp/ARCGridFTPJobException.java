package org.nordugrid.gridftp;

public class ARCGridFTPJobException extends Exception {

    protected int code;

    public ARCGridFTPJobException() {
        super();
        this.code=0;
    }

    public ARCGridFTPJobException(String message) {
        super(message);
        this.code=0;
    }

    public ARCGridFTPJobException(String message,int code) {
        super(message);
        this.code=code;
    }

}

