package com.javahelps.wisdom.extensions.file.source;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.*;

import java.io.EOFException;
import java.util.HashMap;
import java.util.Map;

import static com.javahelps.wisdom.extensions.file.util.Constants.PATH;

@WisdomExtension("file.pcap")
public class PcapFileSource extends Source {

    private final String path;
    private boolean running;
    private InputHandler inputHandler;
    private PcapHandle handle;

    public PcapFileSource(Map<String, ?> properties) {
        super(properties);
        this.path = (String) properties.get(PATH);
        if (this.path == null) {
            throw new WisdomAppValidationException("Required property %s for PcapFile sink not found", PATH);
        }
    }

    private static Map<String, Object> extractTcpPacket(PcapHandle handle, Packet packet) {

        if (!packet.contains(TcpPacket.class)) {
            return null;
        }
        IpPacket.IpHeader ipHeader = packet.get(IpV4Packet.class).getHeader();
        TcpPacket tcpPacket = packet.get(TcpPacket.class);
        TcpPacket.TcpHeader tcpHeader = tcpPacket.getHeader();

        String srcPortName = tcpHeader.getSrcPort().name().toLowerCase();
        String dstPortName = tcpHeader.getDstPort().name().toLowerCase();
        String service = service(srcPortName, dstPortName);

        Map<String, Object> networkPacket = new HashMap<>();
        networkPacket.put("trans_protocol", "tcp");
        networkPacket.put("app_protocol", service);
        networkPacket.put("src_ip", ipHeader.getSrcAddr().getHostAddress());
        networkPacket.put("src_port", tcpHeader.getSrcPort().value().longValue());
        networkPacket.put("dst_ip", ipHeader.getDstAddr().getHostAddress());
        networkPacket.put("dst_port", tcpHeader.getDstPort().value().longValue());
        networkPacket.put("timestamp", handle.getTimestamp().getTime());

        // Set flags
        networkPacket.put("ack", tcpHeader.getAck());
        networkPacket.put("fin", tcpHeader.getFin());
        networkPacket.put("psh", tcpHeader.getPsh());
        networkPacket.put("rst", tcpHeader.getRst());
        networkPacket.put("syn", tcpHeader.getSyn());
        networkPacket.put("urg", tcpHeader.getUrg());

        Packet payload = tcpPacket.getPayload();
        if (payload != null) {
            networkPacket.put("length", payload.length());
            networkPacket.put("data", new String(payload.getRawData()));
        }
        return networkPacket;
    }

    private static Map<String, Object> extractUdpPacket(PcapHandle handle, Packet packet) {

        if (!packet.contains(UdpPacket.class)) {
            return null;
        }

        IpPacket.IpHeader ipHeader = packet.get(IpV4Packet.class).getHeader();
        UdpPacket udpPacket = packet.get(UdpPacket.class);
        UdpPacket.UdpHeader udpHeader = udpPacket.getHeader();

        Map<String, Object> networkPacket = new HashMap<>();
        networkPacket.put("trans_protocol", "udp");
        networkPacket.put("src_ip", ipHeader.getSrcAddr().getHostAddress());
        networkPacket.put("src_port", udpHeader.getSrcPort().value().longValue());
        networkPacket.put("dst_ip", ipHeader.getDstAddr().getHostAddress());
        networkPacket.put("dst_port", udpHeader.getDstPort().value().longValue());
        networkPacket.put("timestamp", handle.getTimestamp().getTime());

        Packet payload = udpPacket.getPayload();
        if (payload != null) {
            networkPacket.put("length", payload.length());
            networkPacket.put("data", new String(payload.getRawData()));
        }
        return networkPacket;
    }

    private static Map<String, Object> extractIcmpPacket(PcapHandle handle, Packet packet) {

        if (!packet.contains(IcmpV4CommonPacket.class)) {
            return null;
        }

        IpPacket.IpHeader ipHeader = packet.get(IpV4Packet.class).getHeader();
        IcmpV4CommonPacket icmpPacket = packet.get(IcmpV4CommonPacket.class);

        Map<String, Object> networkPacket = new HashMap<>();
        networkPacket.put("trans_protocol", "icmp");
        networkPacket.put("src_ip", ipHeader.getSrcAddr().getHostAddress());
        networkPacket.put("dst_ip", ipHeader.getDstAddr().getHostAddress());
        networkPacket.put("timestamp", handle.getTimestamp().getTime());

        Packet payload = icmpPacket.getPayload();
        if (payload != null) {
            networkPacket.put("length", payload.length());
            networkPacket.put("data", new String(payload.getRawData()));
        }
        return networkPacket;
    }

    private static String service(String srcService, String dstService) {
        String service = dstService.toLowerCase();
        if ("unknown".equals(service)) {
            service = srcService.toLowerCase();
        }
        return service;
    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {
        this.inputHandler = wisdomApp.getInputHandler(streamId);
    }

    @Override
    public void start() {
        synchronized (this) {
            if (this.running) {
                return;
            }
            this.running = true;
        }

        try {
            handle = Pcaps.openOffline(path, PcapHandle.TimestampPrecision.NANO);
        } catch (PcapNativeException e) {
            try {
                handle = Pcaps.openOffline(path);
            } catch (PcapNativeException e1) {
                throw new WisdomAppRuntimeException("Could not read pcap file: %s", path);
            }
        }

        if (handle != null) {

            while (true) {
                synchronized (this) {
                    if (!this.running) {
                        break;
                    }
                }
                try {
                    Packet packet = handle.getNextPacketEx();

                    Map<String, Object> networkPacket = extractIcmpPacket(handle, packet);

                    if (networkPacket == null) {
                        networkPacket = extractUdpPacket(handle, packet);
                    }
                    if (networkPacket == null) {
                        networkPacket = extractTcpPacket(handle, packet);
                    }
                    if (networkPacket != null) {
                        this.inputHandler.send(EventGenerator.generate(networkPacket));
                    }
                } catch (EOFException e) {
                    // Reached the end of the file
                    break;
                } catch (Exception ex) {
                    // Do nothing
                }
            }

            handle.close();
        }
        synchronized (this) {
            this.running = false;
        }
    }

    @Override
    public void stop() {
        synchronized (this) {
            this.running = false;
        }
    }
}
