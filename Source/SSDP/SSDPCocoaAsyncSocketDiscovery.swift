//
//  SSDPCocoaAsyncSocketDiscovery.swift
//
//  Copyright (c) 2023 Katoemba Software, (https://rigelian.net/)
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in all
//  copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  SOFTWARE.
//
//  Created by Berrie Kremers on 03/03/2022.
//

import Foundation
import Combine
import CocoaAsyncSocket
import os.log

/// Hook through which the app tells SSDP discovery which network interface to use.
///
/// Without it, SSDP multicast follows the default route. A full-tunnel VPN points that route into
/// its tunnel: M-SEARCH requests never reach the local network and NOTIFY advertisements are not
/// received, so no device is discovered. Pinning the traffic to the physical interface fixes that.
public enum SSDPConfiguration {
    /// Returns the name of the interface SSDP multicast must use (e.g. "en0"), or `nil` to let the
    /// system choose (previous behavior). Evaluated when discovery starts and before every search,
    /// so network changes are followed. Set it once, before discovery starts.
    nonisolated(unsafe) public static var interfaceProvider: (() -> String?)?
}

class SSDPCocoaAsyncSocketDiscovery: SSDPDiscovery {
    /// Socket joined to the SSDP multicast group on port 1900, used to receive
    /// device advertisements (NOTIFY ssdp:alive / ssdp:byebye).
    private var multicastSocket: GCDAsyncUdpSocket?

    /// Socket bound to an ephemeral port, used to send M-SEARCH requests and to
    /// receive the unicast search responses (200 OK). Because M-SEARCH is sent
    /// from this socket, its ephemeral port becomes the source port, and compliant
    /// devices reply to that source port. Some devices (e.g. Asset UPnP media server)
    /// reply to a port of their own choosing rather than 1900; sending from a dedicated
    /// ephemeral socket and receiving on it captures those responses as well.
    private var searchSocket: GCDAsyncUdpSocket?

    /// Interface the multicast socket joined the SSDP group on (`nil` = system default).
    private var joinedInterface: String?

    func startDiscovery(forTypes types: [String]) throws {
        guard multicastSocket == nil else { throw UPnPError.alreadyConnected }

        let interface = SSDPConfiguration.interfaceProvider?()

        // Multicast listening socket: receives NOTIFY advertisements on port 1900.
        // It stays bound to all addresses: on Darwin, a socket bound to a unicast address no longer
        // receives multicast. Only the group membership is tied to the interface.
        let multicastSocket = GCDAsyncUdpSocket(delegate: self, delegateQueue: DispatchQueue.main)
        multicastSocket.setIPv4Enabled(true)
        multicastSocket.setIPv6Enabled(true)

        try multicastSocket.enableReusePort(true)
        try multicastSocket.enableBroadcast(true)
        try multicastSocket.bind(toPort: multicastUDPPort)
        try joinGroup(on: multicastSocket, interface: interface)
        try multicastSocket.beginReceiving()

        // Search socket: sends M-SEARCH from an OS-assigned ephemeral port and
        // receives the unicast responses. beginReceiving() is required so that
        // incoming responses are delivered to the delegate.
        let searchSocket = GCDAsyncUdpSocket(delegate: self, delegateQueue: DispatchQueue.main)
        searchSocket.setIPv4Enabled(true)
        searchSocket.setIPv6Enabled(true)
        try searchSocket.bind(toPort: 0)
        setMulticastInterface(on: searchSocket, interface: interface)
        try searchSocket.beginReceiving()

        self.types = types
        self.multicastSocket = multicastSocket
        self.searchSocket = searchSocket
    }

    func stopDiscovery() {
        guard multicastSocket != nil || searchSocket != nil else { return }

        // Nil out the properties *before* closing, so that the `udpSocketDidClose`
        // callback triggered by `close()` no longer recognises the socket as one of
        // ours (=== check fails) and treats the close as intentional teardown,
        // breaking any close → stopDiscovery recursion.
        let multicast = multicastSocket
        let search = searchSocket
        multicastSocket = nil
        searchSocket = nil
        joinedInterface = nil

        multicast?.close()
        search?.close()

        types = []
    }

    func searchRequest() {
        guard let searchSocket = searchSocket else { return }

        // Follow network changes (Wi-Fi ↔ Ethernet, VPN on/off) before each search.
        refreshInterface()

        // Send the M-SEARCH for every type towards the multicast group on port 1900.
        // Only the source port differs from the listening socket: responses come back
        // to this socket's ephemeral port.
        for type in types {
            if let data = self.searchRequestData(forType: type) {
                searchSocket.send(data, toHost: multicastGroupAddress, port: multicastUDPPort, withTimeout: 3, tag: type.hashValue)
            }
        }
    }

    // MARK: Interface pinning

    /// Joins the SSDP group on `interface`. If that fails, falls back to the system default
    /// interface (previous behavior), so pinning can never make discovery worse than before.
    private func joinGroup(on socket: GCDAsyncUdpSocket, interface: String?) throws {
        if let interface {
            do {
                try socket.joinMulticastGroup(multicastGroupAddress, onInterface: interface)
                joinedInterface = interface
                return
            } catch {
                Logger.swiftUPnP.error("Failed to join SSDP group on \(interface), falling back to default interface: \(error.localizedDescription)")
            }
        }
        try socket.joinMulticastGroup(multicastGroupAddress)
        joinedInterface = nil
    }

    /// Sets the outgoing interface for multicast (IP_MULTICAST_IF). Non-fatal: on failure the
    /// M-SEARCH simply follow the default route, as before.
    private func setMulticastInterface(on socket: GCDAsyncUdpSocket, interface: String?) {
        guard let interface else { return }
        do {
            try socket.sendIPv4Multicast(onInterface: interface)
        } catch {
            Logger.swiftUPnP.error("Failed to set SSDP multicast interface \(interface): \(error.localizedDescription)")
        }
    }

    /// Re-applies the interface given by `SSDPConfiguration.interfaceProvider`: outgoing interface
    /// on the search socket, and group membership on the multicast socket if the interface changed.
    private func refreshInterface() {
        guard let provider = SSDPConfiguration.interfaceProvider else { return }
        let interface = provider()

        if let searchSocket {
            setMulticastInterface(on: searchSocket, interface: interface)
        }

        guard let multicastSocket, interface != joinedInterface else { return }
        if let joinedInterface {
            try? multicastSocket.leaveMulticastGroup(multicastGroupAddress, onInterface: joinedInterface)
        } else {
            try? multicastSocket.leaveMulticastGroup(multicastGroupAddress)
        }
        do {
            try joinGroup(on: multicastSocket, interface: interface)
        } catch {
            Logger.swiftUPnP.error("Failed to rejoin SSDP group: \(error.localizedDescription)")
        }
    }
}

extension SSDPCocoaAsyncSocketDiscovery: GCDAsyncUdpSocketDelegate {
    public func udpSocket(_ sock: GCDAsyncUdpSocket, didNotSendDataWithTag tag: Int, dueToError error: Error?) {
        // A failed M-SEARCH send is transient and must not tear down discovery:
        // doing so would also close the multicast socket and drop passive NOTIFY
        // reception. Log and carry on; the send can be retried on the next search.
        Logger.swiftUPnP.error("Failed to send M-SEARCH (tag \(tag)): \(error?.localizedDescription ?? "unknown error")")
    }

    public func udpSocketDidClose(_ sock: GCDAsyncUdpSocket, withError error: Error?) {
        // Only react to an *unexpected* close. During stopDiscovery() we set the
        // socket properties to nil before/around closing, so a close that no longer
        // corresponds to one of our live sockets is part of intentional teardown and
        // is ignored — this also prevents a close→stopDiscovery→close recursion.
        guard sock === multicastSocket || sock === searchSocket else { return }

        if let error = error {
            Logger.swiftUPnP.error("SSDP socket closed unexpectedly: \(error.localizedDescription)")
        }
        stopDiscovery()
    }

    public func udpSocket(_ sock: GCDAsyncUdpSocket, didReceive data: Data, fromAddress address: Data, withFilterContext filterContext: Any?) {
        // Both sockets share this delegate: multicast NOTIFY messages and unicast
        // search responses are parsed identically by processData.
        processData(data)
    }
}
