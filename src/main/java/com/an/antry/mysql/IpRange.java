package com.an.antry.mysql;

public class IpRange implements Comparable<IpRange> {
    private long startIp;
    private long endIp;

    /**
     * @param startIp
     * @param endIp
     */
    public IpRange(long startIp, long endIp) {
        this.startIp = startIp;
        this.endIp = endIp;
    }

    public IpRange() {
    }

    @Override
    public int compareTo(IpRange o) {
        if (this.startIp > o.getStartIp()) {
            return 1;
        } else if (this.startIp == o.getStartIp()) {
            return 0;
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return "IpRange [startIp=" + startIp + ", endIp=" + endIp + "]";
    }

    public long getStartIp() {
        return startIp;
    }

    public void setStartIp(long startIp) {
        this.startIp = startIp;
    }

    public long getEndIp() {
        return endIp;
    }

    public void setEndIp(long endIp) {
        this.endIp = endIp;
    }
}
