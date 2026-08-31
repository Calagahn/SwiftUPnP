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

    func startDiscovery(forTypes types: [String]) throws {
        guard multicastSocket == nil else { throw UPnPError.alreadyConnected }

        // Multicast listening socket: receives NOTIFY advertisements on port 1900.
        let multicastSocket = GCDAsyncUdpSocket(delegate: self, delegateQueue: DispatchQueue.main)
        multicastSocket.setIPv4Enabled(true)
        multicastSocket.setIPv6Enabled(true)

        try multicastSocket.enableReusePort(true)
        try multicastSocket.enableBroadcast(true)
        try multicastSocket.bind(toPort: multicastUDPPort)
        try multicastSocket.joinMulticastGroup(multicastGroupAddress)
        try multicastSocket.beginReceiving()

        // Search socket: sends M-SEARCH from an OS-assigned ephemeral port and
        // receives the unicast responses. beginReceiving() is required so that
        // incoming responses are delivered to the delegate.
        let searchSocket = GCDAsyncUdpSocket(delegate: self, delegateQueue: DispatchQueue.main)
        searchSocket.setIPv4Enabled(true)
        searchSocket.setIPv6Enabled(true)
        try searchSocket.bind(toPort: 0)
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

        multicast?.close()
        search?.close()

        types = []
    }

    func searchRequest() {
        guard let searchSocket = searchSocket else { return }

        // Send the M-SEARCH for every type towards the multicast group on port 1900.
        // Only the source port differs from the listening socket: responses come back
        // to this socket's ephemeral port.
        for type in types {
            if let data = self.searchRequestData(forType: type) {
                searchSocket.send(data, toHost: multicastGroupAddress, port: multicastUDPPort, withTimeout: 3, tag: type.hashValue)
            }
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
