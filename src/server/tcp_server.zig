const std = @import("std");
const raft = @import("raftz");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");
const wire = @import("../wire.zig");
const blocking_client = @import("../client/blocking.zig");
const TcpTransport = @import("../client/tcp_transport.zig").TcpTransport;
const acl = @import("acl.zig");
const command = @import("command.zig");
const config_mod = @import("config.zig");
const data_tree = @import("data_tree.zig");
const quorum_mod = @import("quorum.zig");

const ConnectionSession = struct {
    id: i64,
    password: [16]u8,
    timeout_ms: i32,
    generation: u64,
};

const ConnectionContext = struct {
    session: ConnectionSession,
    identities: std.ArrayList(acl.Identity) = .empty,

    fn deinit(self: *ConnectionContext, allocator: std.mem.Allocator) void {
        for (self.identities.items) |identity| allocator.free(identity.id);
        self.identities.deinit(allocator);
        self.* = undefined;
    }
};

const SessionMutationResult = struct {
    code: data_tree.ErrorCode,
    zxid: i64,
};

pub const TcpServer = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    quorum: *quorum_mod.Quorum,
    address: std.Io.net.IpAddress,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        quorum: *quorum_mod.Quorum,
        host: []const u8,
        port: u16,
    ) !TcpServer {
        return .{
            .allocator = allocator,
            .io = io,
            .quorum = quorum,
            .address = try std.Io.net.IpAddress.parseIp4(host, port),
        };
    }

    pub fn serve(self: *TcpServer) !void {
        var listener = try self.address.listen(self.io, .{ .reuse_address = true });
        defer listener.deinit(self.io);
        while (true) {
            const stream = try listener.accept(self.io);
            const thread = std.Thread.spawn(.{}, serveThread, .{ self, stream }) catch |err| {
                stream.close(self.io);
                return err;
            };
            thread.detach();
        }
    }

    fn serveThread(self: *TcpServer, stream: std.Io.net.Stream) void {
        self.serveConnection(stream) catch |err| {
            std.log.warn("ZooKeeper client connection closed: {s}", .{@errorName(err)});
        };
    }

    fn serveConnection(self: *TcpServer, stream: std.Io.net.Stream) !void {
        var transport = TcpTransport.fromStream(stream);
        defer transport.close(self.io);

        const connect_payload = try transport.readFrameAlloc(
            self.allocator,
            self.io,
            wire.default_max_payload,
        );
        defer self.allocator.free(connect_payload);
        const connect = try wire.decodeConnectRequest(connect_payload);
        const session = (try self.establishSession(&transport, connect)) orelse return;
        var context = ConnectionContext{ .session = session };
        defer context.deinit(self.allocator);
        const remote_ip = try peerIpv4Alloc(self.allocator, transport.stream);
        context.identities.append(self.allocator, .{ .scheme = "ip", .id = remote_ip }) catch |err| {
            self.allocator.free(remote_ip);
            return err;
        };

        while (true) {
            const payload = try transport.readFrameAlloc(
                self.allocator,
                self.io,
                wire.default_max_payload,
            );
            defer self.allocator.free(payload);
            const request = try wire.requestView(payload);
            const opcode = wire.OpCode.fromInt(request.header.type) orelse {
                try sendError(&transport, self.allocator, self.io, request.header.xid, -1, .unimplemented);
                continue;
            };
            const keep_open = try self.dispatch(
                &transport,
                &context,
                request.header.xid,
                opcode,
                request.body,
            );
            if (!keep_open) return;
        }
    }

    fn establishSession(
        self: *TcpServer,
        transport: *TcpTransport,
        connect: wire.DecodedConnectRequest,
    ) !?ConnectionSession {
        if (connect.value.sessionId == 0) {
            const requested_timeout = @max(connect.value.timeOut, 2_000);
            const negotiated_timeout = @min(requested_timeout, 40_000);
            var attempts: usize = 0;
            while (attempts < 8) : (attempts += 1) {
                var entropy: [32]u8 = undefined;
                try std.Io.randomSecure(self.io, &entropy);
                const raw_id = std.mem.readInt(u64, entropy[0..8], .big) & std.math.maxInt(i64);
                const generation: u64 = 1;
                if (raw_id == 0) continue;
                const session_id: i64 = @intCast(raw_id);
                var password: [16]u8 = undefined;
                @memcpy(&password, entropy[8..24]);
                const result = try self.proposeSession(.{ .open_session = .{
                    .session_id = session_id,
                    .password = &password,
                    .timeout_ms = negotiated_timeout,
                    .tick_grace_ms = @intCast(self.quorum.options.session_reap_interval_ms),
                    .generation = generation,
                } });
                if (result.code == .session_moved) continue;
                if (result.code != .ok) break;
                try sendConnectResponse(transport, self.allocator, self.io, .{
                    .protocolVersion = 0,
                    .timeOut = negotiated_timeout,
                    .sessionId = session_id,
                    .passwd = &password,
                    .readOnly = false,
                }, connect.read_only_supported);
                return .{
                    .id = session_id,
                    .password = password,
                    .timeout_ms = negotiated_timeout,
                    .generation = generation,
                };
            }
            try sendFailedConnect(transport, self.allocator, self.io, connect.read_only_supported);
            return null;
        }

        try self.quorum.linearizableRead();
        const stored = (try self.quorum.machine.getSession(connect.value.sessionId)) orelse {
            try sendFailedConnect(transport, self.allocator, self.io, connect.read_only_supported);
            return null;
        };
        const supplied_password = connect.value.passwd orelse &.{};
        if (supplied_password.len != stored.password.len or
            !std.mem.eql(u8, supplied_password, &stored.password))
        {
            try sendFailedConnect(transport, self.allocator, self.io, connect.read_only_supported);
            return null;
        }
        const new_generation = std.math.add(u64, stored.generation, 1) catch {
            try sendFailedConnect(transport, self.allocator, self.io, connect.read_only_supported);
            return null;
        };
        const result = try self.proposeSession(.{ .move_session = .{
            .session_id = connect.value.sessionId,
            .password = &stored.password,
            .expected_generation = stored.generation,
            .new_generation = new_generation,
        } });
        if (result.code != .ok) {
            try sendFailedConnect(transport, self.allocator, self.io, connect.read_only_supported);
            return null;
        }
        try sendConnectResponse(transport, self.allocator, self.io, .{
            .protocolVersion = 0,
            .timeOut = stored.timeout_ms,
            .sessionId = connect.value.sessionId,
            .passwd = &stored.password,
            .readOnly = false,
        }, connect.read_only_supported);
        return .{
            .id = connect.value.sessionId,
            .password = stored.password,
            .timeout_ms = stored.timeout_ms,
            .generation = new_generation,
        };
    }

    fn proposeSession(self: *TcpServer, mutation: command.Mutation) !SessionMutationResult {
        var proposal = try self.quorum.propose(mutation);
        defer proposal.deinit();
        const result = try command.decodeResult(proposal.bytes);
        if (result.body.len != 0) return error.InvalidProposalResponse;
        return .{ .code = result.code, .zxid = result.zxid };
    }

    fn dispatch(
        self: *TcpServer,
        transport: *TcpTransport,
        context: *ConnectionContext,
        xid: i32,
        opcode: wire.OpCode,
        body: []const u8,
    ) !bool {
        const session = context.session;
        if (opcode != .close_session) {
            const touched = self.proposeSession(.{ .touch_session = .{
                .session_id = session.id,
                .password = &session.password,
                .generation = session.generation,
            } }) catch {
                try sendError(transport, self.allocator, self.io, xid, -1, .connection_loss);
                return false;
            };
            if (touched.code != .ok) {
                try sendError(transport, self.allocator, self.io, xid, touched.zxid, touched.code);
                return false;
            }
        }
        switch (opcode) {
            .ping => {
                try sendReply(transport, self.allocator, self.io, .{
                    .xid = wire.Xid.ping,
                    .zxid = -1,
                    .err = 0,
                }, {});
                return true;
            },
            .close_session => {
                const closed = self.proposeSession(.{ .close_session = .{
                    .session_id = session.id,
                    .password = &session.password,
                    .generation = session.generation,
                } }) catch {
                    try sendError(transport, self.allocator, self.io, xid, -1, .connection_loss);
                    return false;
                };
                if (closed.code != .ok) {
                    try sendError(transport, self.allocator, self.io, xid, closed.zxid, closed.code);
                    return false;
                }
                try sendReply(transport, self.allocator, self.io, .{
                    .xid = xid,
                    .zxid = closed.zxid,
                    .err = 0,
                }, {});
                return false;
            },
            .auth => try self.authenticate(transport, context, body),
            .create, .create2 => try self.create(transport, session, context.identities.items, xid, opcode, body),
            .delete => try self.delete(transport, session, context.identities.items, xid, body),
            .set_acl => try self.setAcl(transport, session, context.identities.items, xid, body),
            .set_data => try self.setData(transport, session, context.identities.items, xid, body),
            .exists => try self.exists(transport, session, context.identities.items, xid, body),
            .get_acl => try self.getAcl(transport, session, context.identities.items, xid, body),
            .get_data => try self.getData(transport, session, context.identities.items, xid, body),
            .get_children, .get_children2 => try self.getChildren(transport, session, context.identities.items, xid, opcode, body),
            .sync => try self.sync(transport, session, xid, body),
            else => try sendError(transport, self.allocator, self.io, xid, -1, .unimplemented),
        }
        return true;
    }

    fn authenticate(
        self: *TcpServer,
        transport: *TcpTransport,
        context: *ConnectionContext,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.AuthPacket, bytes, self.allocator);
        const scheme = request.scheme orelse
            return sendError(transport, self.allocator, self.io, wire.Xid.auth, -1, .auth_failed);
        if (std.mem.eql(u8, scheme, "ip")) {
            return sendReply(transport, self.allocator, self.io, .{
                .xid = wire.Xid.auth,
                .zxid = -1,
                .err = 0,
            }, {});
        }
        if (!std.mem.eql(u8, scheme, "digest")) {
            return sendError(transport, self.allocator, self.io, wire.Xid.auth, -1, .auth_failed);
        }
        const credentials = request.auth orelse
            return sendError(transport, self.allocator, self.io, wire.Xid.auth, -1, .auth_failed);
        const identity = acl.digestIdentity(self.allocator, credentials) catch
            return sendError(transport, self.allocator, self.io, wire.Xid.auth, -1, .auth_failed);
        for (context.identities.items) |existing| {
            if (std.mem.eql(u8, existing.scheme, "digest") and std.mem.eql(u8, existing.id, identity)) {
                self.allocator.free(identity);
                return sendReply(transport, self.allocator, self.io, .{
                    .xid = wire.Xid.auth,
                    .zxid = -1,
                    .err = 0,
                }, {});
            }
        }
        context.identities.append(self.allocator, .{ .scheme = "digest", .id = identity }) catch |err| {
            self.allocator.free(identity);
            return err;
        };
        return sendReply(transport, self.allocator, self.io, .{
            .xid = wire.Xid.auth,
            .zxid = -1,
            .err = 0,
        }, {});
    }

    fn create(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        identities: []const acl.Identity,
        xid: i32,
        opcode: wire.OpCode,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.CreateRequest, bytes, self.allocator);
        defer jute.deinitDecoded(request, self.allocator);
        const path = request.path orelse return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            -1,
            .bad_arguments,
        );
        if (request.flags < 0 or request.flags > 3) return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            -1,
            .unimplemented,
        );
        const acl_blob = acl.normalize(self.allocator, request.acl, identities) catch
            return sendError(transport, self.allocator, self.io, xid, -1, .invalid_acl);
        defer self.allocator.free(acl_blob);
        const identities_blob = try acl.encodeIdentities(self.allocator, identities);
        defer self.allocator.free(identities_blob);
        var proposal = self.quorum.propose(.{ .create = .{
            .path = path,
            .data = request.data orelse &.{},
            .time_ms = std.Io.Clock.real.now(self.io).toMilliseconds(),
            .ephemeral = (request.flags & 1) != 0,
            .session_id = session.id,
            .session_generation = session.generation,
            .sequential = (request.flags & 2) != 0,
            .acl = acl_blob,
            .identities = identities_blob,
        } }) catch return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            -1,
            .connection_loss,
        );
        defer proposal.deinit();
        const result = try command.decodeResult(proposal.bytes);
        if (result.code != .ok) return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            result.zxid,
            result.code,
        );
        var reader = jute.Reader.init(result.body);
        const response = try jute.deserialize(protocol.proto.Create2Response, &reader, self.allocator);
        if (reader.remaining() != 0) return error.InvalidProposalResponse;
        if (opcode == .create2) {
            try sendReply(transport, self.allocator, self.io, .{
                .xid = xid,
                .zxid = result.zxid,
                .err = 0,
            }, response);
        } else {
            try sendReply(transport, self.allocator, self.io, .{
                .xid = xid,
                .zxid = result.zxid,
                .err = 0,
            }, protocol.proto.CreateResponse{ .path = response.path });
        }
    }

    fn delete(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        identities: []const acl.Identity,
        xid: i32,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.DeleteRequest, bytes, self.allocator);
        const identities_blob = try acl.encodeIdentities(self.allocator, identities);
        defer self.allocator.free(identities_blob);
        var proposal = self.quorum.propose(.{ .delete = .{
            .path = request.path orelse return sendError(
                transport,
                self.allocator,
                self.io,
                xid,
                -1,
                .bad_arguments,
            ),
            .expected_version = request.version,
            .session_id = session.id,
            .session_generation = session.generation,
            .identities = identities_blob,
        } }) catch return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            -1,
            .connection_loss,
        );
        defer proposal.deinit();
        const result = try command.decodeResult(proposal.bytes);
        if (result.code != .ok) return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            result.zxid,
            result.code,
        );
        try sendReply(transport, self.allocator, self.io, .{
            .xid = xid,
            .zxid = result.zxid,
            .err = 0,
        }, {});
    }

    fn setAcl(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        identities: []const acl.Identity,
        xid: i32,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.SetACLRequest, bytes, self.allocator);
        defer jute.deinitDecoded(request, self.allocator);
        const path = request.path orelse
            return sendError(transport, self.allocator, self.io, xid, -1, .bad_arguments);
        const acl_blob = acl.normalize(self.allocator, request.acl, identities) catch
            return sendError(transport, self.allocator, self.io, xid, -1, .invalid_acl);
        defer self.allocator.free(acl_blob);
        const identities_blob = try acl.encodeIdentities(self.allocator, identities);
        defer self.allocator.free(identities_blob);
        var proposal = self.quorum.propose(.{ .set_acl = .{
            .path = path,
            .acl = acl_blob,
            .expected_version = request.version,
            .session_id = session.id,
            .session_generation = session.generation,
            .identities = identities_blob,
        } }) catch return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            -1,
            .connection_loss,
        );
        defer proposal.deinit();
        const result = try command.decodeResult(proposal.bytes);
        if (result.code != .ok) {
            return sendError(transport, self.allocator, self.io, xid, result.zxid, result.code);
        }
        var reader = jute.Reader.init(result.body);
        const response = try jute.deserialize(protocol.proto.SetACLResponse, &reader, self.allocator);
        if (reader.remaining() != 0) return error.InvalidProposalResponse;
        try sendReply(transport, self.allocator, self.io, .{
            .xid = xid,
            .zxid = result.zxid,
            .err = 0,
        }, response);
    }

    fn setData(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        identities: []const acl.Identity,
        xid: i32,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.SetDataRequest, bytes, self.allocator);
        const identities_blob = try acl.encodeIdentities(self.allocator, identities);
        defer self.allocator.free(identities_blob);
        var proposal = self.quorum.propose(.{ .set_data = .{
            .path = request.path orelse return sendError(
                transport,
                self.allocator,
                self.io,
                xid,
                -1,
                .bad_arguments,
            ),
            .data = request.data orelse &.{},
            .expected_version = request.version,
            .time_ms = std.Io.Clock.real.now(self.io).toMilliseconds(),
            .session_id = session.id,
            .session_generation = session.generation,
            .identities = identities_blob,
        } }) catch return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            -1,
            .connection_loss,
        );
        defer proposal.deinit();
        const result = try command.decodeResult(proposal.bytes);
        if (result.code != .ok) return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            result.zxid,
            result.code,
        );
        var reader = jute.Reader.init(result.body);
        const response = try jute.deserialize(protocol.proto.SetDataResponse, &reader, self.allocator);
        if (reader.remaining() != 0) return error.InvalidProposalResponse;
        try sendReply(transport, self.allocator, self.io, .{
            .xid = xid,
            .zxid = result.zxid,
            .err = 0,
        }, response);
    }

    fn exists(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        identities: []const acl.Identity,
        xid: i32,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.ExistsRequest, bytes, self.allocator);
        const identities_blob = try acl.encodeIdentities(self.allocator, identities);
        defer self.allocator.free(identities_blob);
        try self.quorum.linearizableRead();
        const zxid = appliedZxid(self.quorum);
        const path = request.path orelse return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            zxid,
            .bad_arguments,
        );
        const maybe_stat = self.quorum.machine.existsAuthorizedForSession(
            session.id,
            session.generation,
            path,
            identities_blob,
        ) catch |err| return sendMappedSessionError(
            transport,
            self.allocator,
            self.io,
            xid,
            zxid,
            err,
        );
        const stat = maybe_stat orelse
            return sendError(transport, self.allocator, self.io, xid, zxid, .no_node);
        try sendReply(transport, self.allocator, self.io, .{ .xid = xid, .zxid = zxid, .err = 0 }, protocol.proto.ExistsResponse{ .stat = stat });
    }

    fn getAcl(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        identities: []const acl.Identity,
        xid: i32,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.GetACLRequest, bytes, self.allocator);
        const path = request.path orelse
            return sendError(transport, self.allocator, self.io, xid, -1, .bad_arguments);
        const identities_blob = try acl.encodeIdentities(self.allocator, identities);
        defer self.allocator.free(identities_blob);
        try self.quorum.linearizableRead();
        const zxid = appliedZxid(self.quorum);
        var result = (self.quorum.machine.getAclForSession(
            self.allocator,
            session.id,
            session.generation,
            path,
            identities_blob,
        ) catch |err| return sendMappedSessionError(
            transport,
            self.allocator,
            self.io,
            xid,
            zxid,
            err,
        )) orelse return sendError(transport, self.allocator, self.io, xid, zxid, .no_node);
        defer result.deinit(self.allocator);
        const entries = try acl.decodeViews(self.allocator, result.blob);
        defer self.allocator.free(entries);
        var redacted_ids: std.ArrayList([]u8) = .empty;
        defer {
            for (redacted_ids.items) |id| self.allocator.free(id);
            redacted_ids.deinit(self.allocator);
        }
        if (result.redact_digest) {
            for (entries) |*entry| {
                const scheme = entry.id.scheme orelse continue;
                const id = entry.id.id orelse continue;
                if (!std.mem.eql(u8, scheme, "digest")) continue;
                const colon = std.mem.indexOfScalar(u8, id, ':') orelse continue;
                const redacted = try self.allocator.alloc(u8, colon + 2);
                @memcpy(redacted[0 .. colon + 1], id[0 .. colon + 1]);
                redacted[colon + 1] = 'x';
                redacted_ids.append(self.allocator, redacted) catch |err| {
                    self.allocator.free(redacted);
                    return err;
                };
                entry.id.id = redacted;
            }
        }
        try sendReply(transport, self.allocator, self.io, .{
            .xid = xid,
            .zxid = zxid,
            .err = 0,
        }, protocol.proto.GetACLResponse{ .acl = entries, .stat = result.stat });
    }

    fn getData(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        identities: []const acl.Identity,
        xid: i32,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.GetDataRequest, bytes, self.allocator);
        const identities_blob = try acl.encodeIdentities(self.allocator, identities);
        defer self.allocator.free(identities_blob);
        try self.quorum.linearizableRead();
        const zxid = appliedZxid(self.quorum);
        const path = request.path orelse return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            zxid,
            .bad_arguments,
        );
        var result = (self.quorum.machine.getDataAuthorizedForSession(
            self.allocator,
            session.id,
            session.generation,
            path,
            identities_blob,
        ) catch |err| return sendMappedSessionError(
            transport,
            self.allocator,
            self.io,
            xid,
            zxid,
            err,
        )) orelse return sendError(transport, self.allocator, self.io, xid, zxid, .no_node);
        defer result.deinit(self.allocator);
        try sendReply(transport, self.allocator, self.io, .{ .xid = xid, .zxid = zxid, .err = 0 }, protocol.proto.GetDataResponse{ .data = result.data, .stat = result.stat });
    }

    fn getChildren(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        identities: []const acl.Identity,
        xid: i32,
        opcode: wire.OpCode,
        bytes: []const u8,
    ) !void {
        const path: ?[]const u8 = if (opcode == .get_children) blk: {
            const request = try decodeBody(protocol.proto.GetChildrenRequest, bytes, self.allocator);
            break :blk request.path;
        } else blk: {
            const request = try decodeBody(protocol.proto.GetChildren2Request, bytes, self.allocator);
            break :blk request.path;
        };
        const identities_blob = try acl.encodeIdentities(self.allocator, identities);
        defer self.allocator.free(identities_blob);
        try self.quorum.linearizableRead();
        const zxid = appliedZxid(self.quorum);
        const resolved_path = path orelse return sendError(
            transport,
            self.allocator,
            self.io,
            xid,
            zxid,
            .bad_arguments,
        );
        var result = (self.quorum.machine.getChildrenAuthorizedForSession(
            self.allocator,
            session.id,
            session.generation,
            resolved_path,
            identities_blob,
        ) catch |err| return sendMappedSessionError(
            transport,
            self.allocator,
            self.io,
            xid,
            zxid,
            err,
        )) orelse return sendError(transport, self.allocator, self.io, xid, zxid, .no_node);
        defer result.deinit(self.allocator);
        const children = try self.allocator.alloc(?[]const u8, result.names.len);
        defer self.allocator.free(children);
        for (result.names, 0..) |name, index| children[index] = name;
        if (opcode == .get_children) {
            try sendReply(transport, self.allocator, self.io, .{ .xid = xid, .zxid = zxid, .err = 0 }, protocol.proto.GetChildrenResponse{ .children = children });
        } else {
            try sendReply(transport, self.allocator, self.io, .{ .xid = xid, .zxid = zxid, .err = 0 }, protocol.proto.GetChildren2Response{ .children = children, .stat = result.stat });
        }
    }

    fn sync(
        self: *TcpServer,
        transport: *TcpTransport,
        session: ConnectionSession,
        xid: i32,
        bytes: []const u8,
    ) !void {
        const request = try decodeBody(protocol.proto.SyncRequest, bytes, self.allocator);
        try self.quorum.linearizableRead();
        const zxid = appliedZxid(self.quorum);
        self.quorum.machine.validateSession(session.id, session.generation) catch |err|
            return sendMappedSessionError(transport, self.allocator, self.io, xid, zxid, err);
        try sendReply(transport, self.allocator, self.io, .{ .xid = xid, .zxid = zxid, .err = 0 }, protocol.proto.SyncResponse{ .path = request.path });
    }
};

fn decodeBody(comptime T: type, bytes: []const u8, allocator: std.mem.Allocator) !T {
    var reader = jute.Reader.init(bytes);
    const value = try jute.deserialize(T, &reader, allocator);
    errdefer jute.deinitDecoded(value, allocator);
    if (reader.remaining() != 0) return error.TrailingData;
    return value;
}

fn sendFailedConnect(
    transport: *TcpTransport,
    allocator: std.mem.Allocator,
    io: std.Io,
    include_read_only: bool,
) !void {
    try sendConnectResponse(transport, allocator, io, .{
        .protocolVersion = 0,
        .timeOut = 0,
        .sessionId = 0,
        .passwd = &.{},
        .readOnly = false,
    }, include_read_only);
}

fn sendConnectResponse(
    transport: *TcpTransport,
    allocator: std.mem.Allocator,
    io: std.Io,
    response: protocol.proto.ConnectResponse,
    include_read_only: bool,
) !void {
    var writer = jute.Writer.init(allocator);
    defer writer.deinit();
    try wire.encodeConnectResponse(&writer, response, include_read_only);
    try transport.writeFrame(io, writer.bytes());
}

fn sendReply(
    transport: *TcpTransport,
    allocator: std.mem.Allocator,
    io: std.Io,
    header: protocol.proto.ReplyHeader,
    body: anytype,
) !void {
    var writer = jute.Writer.init(allocator);
    defer writer.deinit();
    try wire.encodeReply(&writer, header, body);
    try transport.writeFrame(io, writer.bytes());
}

fn sendMappedSessionError(
    transport: *TcpTransport,
    allocator: std.mem.Allocator,
    io: std.Io,
    xid: i32,
    zxid: i64,
    err: anyerror,
) !void {
    const code: data_tree.ErrorCode = switch (err) {
        error.SessionExpired => .session_expired,
        error.SessionMoved => .session_moved,
        error.NoAuth => .no_auth,
        else => return err,
    };
    return sendError(transport, allocator, io, xid, zxid, code);
}

fn sendError(
    transport: *TcpTransport,
    allocator: std.mem.Allocator,
    io: std.Io,
    xid: i32,
    zxid: i64,
    code: data_tree.ErrorCode,
) !void {
    return sendReply(transport, allocator, io, .{
        .xid = xid,
        .zxid = zxid,
        .err = @intFromEnum(code),
    }, {});
}

fn appliedZxid(quorum: *quorum_mod.Quorum) i64 {
    return std.math.cast(i64, quorum.machine.appliedIndex()) orelse std.math.maxInt(i64);
}

fn peerIpv4Alloc(
    allocator: std.mem.Allocator,
    stream: std.Io.net.Stream,
) ![]u8 {
    var address: std.posix.sockaddr.storage = undefined;
    var length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
    try std.posix.getpeername(stream.socket.handle, @ptrCast(&address), &length);
    if (address.family != std.posix.AF.INET) return error.UnsupportedPeerAddress;
    const ipv4: *const std.posix.sockaddr.in = @ptrCast(@alignCast(&address));
    const bytes = std.mem.asBytes(&ipv4.addr);
    return std.fmt.allocPrint(allocator, "{}.{}.{}.{}", .{
        bytes[0], bytes[1], bytes[2], bytes[3],
    });
}

fn reserveTestPort(io: std.Io) !u16 {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        listener.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    return std.mem.bigToNative(u16, local_address.port);
}

fn serveOneConnection(server: *TcpServer, listener: *std.Io.net.Server) !void {
    const stream = try listener.accept(server.io);
    try server.serveConnection(stream);
}

fn expectResponse(client: *blocking_client.BlockingClient, xid: i32) !blocking_client.Inbound {
    const inbound = try client.receive();
    try std.testing.expectEqual(xid, inbound.header.xid);
    try std.testing.expectEqual(@as(i32, 0), inbound.header.err);
    return inbound;
}

test "ZooKeeper TCP server serves replicated CRUD requests" {
    const testing = std.testing;
    try raft.log.initGlobal(std.heap.smp_allocator, testing.io, false);
    defer raft.log.deinitGlobal(std.heap.smp_allocator);

    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(root_path);
    const raft_port = try reserveTestPort(testing.io);
    const client_port = try reserveTestPort(testing.io);
    const raft_endpoint = try std.fmt.allocPrint(testing.allocator, "127.0.0.1:{}", .{raft_port});
    defer testing.allocator.free(raft_endpoint);
    const client_endpoint = try std.fmt.allocPrint(testing.allocator, "127.0.0.1:{}", .{client_port});
    defer testing.allocator.free(client_endpoint);
    const peer = try std.fmt.allocPrint(testing.allocator, "1={s}", .{raft_endpoint});
    defer testing.allocator.free(peer);
    const arguments = [_][]const u8{
        "--node-id",       "1",
        "--cluster-id",    "0198f54d-5c2a-7000-8000-000000000002",
        "--client-listen", client_endpoint,
        "--raft-listen",   raft_endpoint,
        "--data-dir",      root_path,
        "--peer",          peer,
    };
    var config = try config_mod.parse(testing.allocator, &arguments);
    defer config.deinit();
    const quorum = try quorum_mod.Quorum.create(std.heap.smp_allocator, testing.io, &config, .{
        .tick_interval_ms = 10,
        .election_tick = 5,
        .heartbeat_tick = 1,
        .proposal_timeout_ticks = 100,
        .read_index_timeout_ticks = 100,
        .snapshot_entries_threshold = 10,
        .session_reap_interval_ms = 10,
    });
    defer quorum.deinit();
    var attempts: usize = 0;
    while (quorum.status().leader_id == 0 and attempts < 500) : (attempts += 1) {
        try testing.io.sleep(.fromMilliseconds(10), .awake);
    }
    try testing.expect(quorum.status().leader_id != 0);

    const listen_address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", client_port);
    var listener = try listen_address.listen(testing.io, .{ .reuse_address = true });
    defer listener.deinit(testing.io);
    var server = try TcpServer.init(
        std.heap.smp_allocator,
        testing.io,
        quorum,
        "127.0.0.1",
        client_port,
    );
    var server_future = testing.io.async(serveOneConnection, .{ &server, &listener });
    defer server_future.cancel(testing.io) catch {};

    const one_second: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(1),
        .clock = .awake,
    } };
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", client_port);
    var client = try blocking_client.BlockingClient.connectAddress(
        testing.allocator,
        testing.io,
        address,
        .{ .handshake_timeout = one_second, .io_timeout = one_second },
        .{},
    );
    var client_active = true;
    defer if (client_active) client.deinit();

    const create_xid = try client.sendRequest(.create, protocol.proto.CreateRequest{
        .path = "/app",
        .data = "one",
        .acl = &.{.{ .perms = acl.all, .id = .{ .scheme = "world", .id = "anyone" } }},
        .flags = 0,
    });
    var create_reply = try expectResponse(&client, create_xid);
    defer create_reply.deinit();
    const create_response = try wire.decodeFrameRecord(
        protocol.proto.CreateResponse,
        create_reply.body(),
        testing.allocator,
    );
    try testing.expectEqualStrings("/app", create_response.path.?);

    const create2_xid = try client.sendRequest(.create2, protocol.proto.CreateRequest{
        .path = "/temporary",
        .data = "created-with-stat",
        .acl = &.{.{ .perms = acl.all, .id = .{ .scheme = "world", .id = "anyone" } }},
        .flags = 0,
    });
    var create2_reply = try expectResponse(&client, create2_xid);
    defer create2_reply.deinit();
    const create2_response = try wire.decodeFrameRecord(
        protocol.proto.Create2Response,
        create2_reply.body(),
        testing.allocator,
    );
    try testing.expectEqualStrings("/temporary", create2_response.path.?);
    try testing.expect(create2_response.stat.czxid > 0);

    const temporary_delete_xid = try client.sendRequest(.delete, protocol.proto.DeleteRequest{
        .path = "/temporary",
        .version = 0,
    });
    var temporary_delete_reply = try expectResponse(&client, temporary_delete_xid);
    defer temporary_delete_reply.deinit();

    const set_xid = try client.sendRequest(.set_data, protocol.proto.SetDataRequest{
        .path = "/app",
        .data = "two",
        .version = 0,
    });
    var set_reply = try expectResponse(&client, set_xid);
    defer set_reply.deinit();
    const set_response = try wire.decodeFrameRecord(
        protocol.proto.SetDataResponse,
        set_reply.body(),
        testing.allocator,
    );
    try testing.expectEqual(@as(i32, 1), set_response.stat.version);

    const get_xid = try client.sendRequest(.get_data, protocol.proto.GetDataRequest{
        .path = "/app",
        .watch = false,
    });
    var get_reply = try expectResponse(&client, get_xid);
    defer get_reply.deinit();
    const get_response = try wire.decodeFrameRecord(
        protocol.proto.GetDataResponse,
        get_reply.body(),
        testing.allocator,
    );
    try testing.expectEqualStrings("two", get_response.data.?);

    const children_xid = try client.sendRequest(.get_children, protocol.proto.GetChildrenRequest{
        .path = "/",
        .watch = false,
    });
    var children_reply = try expectResponse(&client, children_xid);
    defer children_reply.deinit();
    const children_response = try wire.decodeFrameRecord(
        protocol.proto.GetChildrenResponse,
        children_reply.body(),
        testing.allocator,
    );
    defer jute.deinitDecoded(children_response, testing.allocator);
    try testing.expectEqual(@as(usize, 1), children_response.children.?.len);
    try testing.expectEqualStrings("app", children_response.children.?[0].?);

    const sequential_xid = try client.sendRequest(.create2, protocol.proto.CreateRequest{
        .path = "/member-",
        .data = "ordered",
        .acl = &.{.{ .perms = acl.all, .id = .{ .scheme = "world", .id = "anyone" } }},
        .flags = 2,
    });
    var sequential_reply = try expectResponse(&client, sequential_xid);
    defer sequential_reply.deinit();
    const sequential_response = try wire.decodeFrameRecord(
        protocol.proto.Create2Response,
        sequential_reply.body(),
        testing.allocator,
    );
    try testing.expectEqualStrings("/member-0000000003", sequential_response.path.?);

    const ip_secure_xid = try client.sendRequest(.create, protocol.proto.CreateRequest{
        .path = "/ip-secure",
        .data = "loopback-only",
        .acl = &.{.{ .perms = acl.read, .id = .{
            .scheme = "ip",
            .id = "127.0.0.0/8",
        } }},
        .flags = 0,
    });
    var ip_secure_reply = try expectResponse(&client, ip_secure_xid);
    defer ip_secure_reply.deinit();
    const ip_get_xid = try client.sendRequest(.get_data, protocol.proto.GetDataRequest{
        .path = "/ip-secure",
        .watch = false,
    });
    var ip_get_reply = try expectResponse(&client, ip_get_xid);
    defer ip_get_reply.deinit();

    const delete_xid = try client.sendRequest(.delete, protocol.proto.DeleteRequest{
        .path = "/app",
        .version = 1,
    });
    var delete_reply = try expectResponse(&client, delete_xid);
    defer delete_reply.deinit();
    try testing.expectEqual(@as(usize, 0), delete_reply.body().len);

    const ephemeral_xid = try client.sendRequest(.create, protocol.proto.CreateRequest{
        .path = "/ephemeral-",
        .data = "session-owned",
        .acl = &.{.{ .perms = acl.all, .id = .{ .scheme = "world", .id = "anyone" } }},
        .flags = 3,
    });
    var ephemeral_reply = try expectResponse(&client, ephemeral_xid);
    defer ephemeral_reply.deinit();
    const ephemeral_response = try wire.decodeFrameRecord(
        protocol.proto.CreateResponse,
        ephemeral_reply.body(),
        testing.allocator,
    );
    const ephemeral_path = ephemeral_response.path.?;
    try testing.expect(std.mem.startsWith(u8, ephemeral_path, "/ephemeral-"));

    try client.sendAuth("digest", "user:password");
    var auth_reply = try client.receive();
    defer auth_reply.deinit();
    try testing.expectEqual(wire.Xid.auth, auth_reply.header.xid);
    try testing.expectEqual(@as(i32, 0), auth_reply.header.err);

    const secure_xid = try client.sendRequest(.create, protocol.proto.CreateRequest{
        .path = "/secure",
        .data = "private",
        .acl = &.{.{ .perms = acl.all, .id = .{ .scheme = "auth", .id = "" } }},
        .flags = 0,
    });
    var secure_reply = try expectResponse(&client, secure_xid);
    defer secure_reply.deinit();

    const get_acl_xid = try client.sendRequest(.get_acl, protocol.proto.GetACLRequest{ .path = "/secure" });
    var get_acl_reply = try expectResponse(&client, get_acl_xid);
    defer get_acl_reply.deinit();
    const get_acl_response = try wire.decodeFrameRecord(
        protocol.proto.GetACLResponse,
        get_acl_reply.body(),
        testing.allocator,
    );
    defer jute.deinitDecoded(get_acl_response, testing.allocator);
    try testing.expectEqualStrings("digest", get_acl_response.acl.?[0].id.scheme.?);
    try testing.expectEqualStrings("user:tpUq/4Pn5A64fVZyQ0gOJ8ZWqkY=", get_acl_response.acl.?[0].id.id.?);

    const set_acl_xid = try client.sendRequest(.set_acl, protocol.proto.SetACLRequest{
        .path = "/secure",
        .acl = &.{.{ .perms = acl.read, .id = .{
            .scheme = "digest",
            .id = "user:tpUq/4Pn5A64fVZyQ0gOJ8ZWqkY=",
        } }},
        .version = 0,
    });
    var set_acl_reply = try expectResponse(&client, set_acl_xid);
    defer set_acl_reply.deinit();
    const set_acl_response = try wire.decodeFrameRecord(
        protocol.proto.SetACLResponse,
        set_acl_reply.body(),
        testing.allocator,
    );
    try testing.expectEqual(@as(i32, 1), set_acl_response.stat.aversion);

    const redacted_acl_xid = try client.sendRequest(.get_acl, protocol.proto.GetACLRequest{ .path = "/secure" });
    var redacted_acl_reply = try expectResponse(&client, redacted_acl_xid);
    defer redacted_acl_reply.deinit();
    const redacted_acl_response = try wire.decodeFrameRecord(
        protocol.proto.GetACLResponse,
        redacted_acl_reply.body(),
        testing.allocator,
    );
    defer jute.deinitDecoded(redacted_acl_response, testing.allocator);
    try testing.expectEqualStrings("user:x", redacted_acl_response.acl.?[0].id.id.?);

    const session_id = client.session.session_id;
    var session_password: [16]u8 = undefined;
    @memcpy(&session_password, client.session.passwd);

    var resume_future = testing.io.async(serveOneConnection, .{ &server, &listener });
    defer resume_future.cancel(testing.io) catch {};
    var resumed = try blocking_client.BlockingClient.connectAddress(
        testing.allocator,
        testing.io,
        address,
        .{ .handshake_timeout = one_second, .io_timeout = one_second },
        .{
            .session_id = session_id,
            .passwd = &session_password,
        },
    );
    defer resumed.deinit();
    try testing.expectEqual(session_id, resumed.session.session_id);
    const unauthorized_xid = try resumed.sendRequest(.get_data, protocol.proto.GetDataRequest{
        .path = "/secure",
        .watch = false,
    });
    var unauthorized_reply = try resumed.receive();
    defer unauthorized_reply.deinit();
    try testing.expectEqual(unauthorized_xid, unauthorized_reply.header.xid);
    try testing.expectEqual(@as(i32, @intFromEnum(data_tree.ErrorCode.no_auth)), unauthorized_reply.header.err);

    try resumed.sendAuth("digest", "user:password");
    var resumed_auth_reply = try resumed.receive();
    defer resumed_auth_reply.deinit();
    try testing.expectEqual(wire.Xid.auth, resumed_auth_reply.header.xid);
    try testing.expectEqual(@as(i32, 0), resumed_auth_reply.header.err);
    const authorized_xid = try resumed.sendRequest(.get_data, protocol.proto.GetDataRequest{
        .path = "/secure",
        .watch = false,
    });
    var authorized_reply = try expectResponse(&resumed, authorized_xid);
    defer authorized_reply.deinit();

    try client.sendPing();
    var fenced_reply = try client.receive();
    defer fenced_reply.deinit();
    try testing.expectEqual(@as(i32, @intFromEnum(data_tree.ErrorCode.session_moved)), fenced_reply.header.err);
    client.deinit();
    client_active = false;
    try server_future.await(testing.io);
    try testing.expect((try quorum.machine.getSession(session_id)) != null);
    try testing.expect((try quorum.machine.exists(ephemeral_path)) != null);

    try resumed.close(one_second);
    try resume_future.await(testing.io);
    try testing.expect((try quorum.machine.getSession(session_id)) == null);
    try testing.expect((try quorum.machine.exists(ephemeral_path)) == null);
    try quorum.shutdown();
}
