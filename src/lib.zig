const std = @import("std");

const log = std.log.scoped(.btree);

//

pub const Error = std.mem.Allocator.Error;

pub fn Config(comptime K: type) type {
    return struct {
        // node_len: usize = 100,
        node_size: union(enum) {
            /// maximum size of all nodes in bytes
            bytes: usize,
            /// min=nodes-1 max=nodes*2-1
            nodes: usize,
        } = .{ .bytes = 0x1000 },
        search: enum { linear, binary } = .binary,
        cmp: fn (K, K) std.math.Order = defaultCmp,

        pub fn defaultCmp(lhs: K, rhs: K) std.math.Order {
            return std.math.order(lhs, rhs);
        }
    };
}

pub fn BTreeMap(comptime K: type, comptime V: type, comptime cfg: Config(K)) type {
    return struct {
        root: ?*AnyNode = null,
        depth: usize = 0,
        size: usize = 0,

        const Self = @This();

        pub const KV = struct {
            key: K,
            value: V,
        };

        pub const LeafNode = struct {
            used: usize = 0,
            keys: [MAX]K = undefined,
            vals: [MAX]V = undefined,

            const LIMIT: usize = (cfg.node_size.bytes - @sizeOf(usize)) /
                (@sizeOf(K) + @sizeOf(V));
            const N: usize = switch (cfg.node_size) {
                .bytes => (LIMIT + 1) / 2,
                .nodes => cfg.node_size.nodes,
            };

            pub const MAX: usize = N * 2 - 1;
            pub const MIN: usize = N - 1;
        };

        pub const BranchNode = struct {
            used: usize = 0,
            ptrs: [MAX + 1]*AnyNode = undefined,
            keys: [MAX]K = undefined,
            vals: [MAX]V = undefined,

            const LIMIT: usize = (cfg.node_size.bytes - @sizeOf(usize) - @sizeOf(?*AnyNode)) /
                (@sizeOf(K) + @sizeOf(V) + @sizeOf(*AnyNode));
            const N: usize = switch (cfg.node_size) {
                .bytes => (LIMIT + 1) / 2,
                .nodes => cfg.node_size.nodes,
            };

            pub const MAX: usize = N * 2 - 1;
            pub const MIN: usize = N - 1;
        };

        const AnyNode = anyopaque;

        comptime {
            if (cfg.node_size == .bytes) {
                std.debug.assert(@sizeOf(LeafNode) <= cfg.node_size.bytes);
                std.debug.assert(@sizeOf(BranchNode) <= cfg.node_size.bytes);
            }
            if (LeafNode.MIN == 0 or BranchNode.MIN == 0)
                @compileError("node_size too small");
        }

        const Node = struct {
            ptr: *AnyNode,
            used: *usize,
            keys: []K,
            vals: []V,
            ptrs: []*AnyNode,
            max: usize,
            min: usize,

            fn fromLeaf(node: *LeafNode) Node {
                return .{
                    .ptr = node,
                    .used = &node.used,
                    .keys = node.keys[0..],
                    .vals = node.vals[0..],
                    .ptrs = &.{},
                    .max = LeafNode.MAX,
                    .min = LeafNode.MIN,
                };
            }

            fn fromBranch(node: *BranchNode) Node {
                return .{
                    .ptr = node,
                    .used = &node.used,
                    .keys = node.keys[0..],
                    .vals = node.vals[0..],
                    .ptrs = node.ptrs[0..],
                    .max = BranchNode.MAX,
                    .min = BranchNode.MIN,
                };
            }
        };

        fn getNode(node_ptr: *anyopaque, depth: usize) Node {
            if (depth == 0) {
                return Node.fromLeaf(@ptrCast(@alignCast(node_ptr)));
            } else {
                return Node.fromBranch(@ptrCast(@alignCast(node_ptr)));
            }
        }

        fn insertArr(comptime T: type, arr: []T, len: usize, i: usize, val: T) void {
            std.debug.assert(i <= len and len <= arr.len);
            std.mem.copyBackwards(T, arr[i + 1 .. len + 1], arr[i..len]);
            arr[i] = val;
        }

        fn removeArr(comptime T: type, arr: []T, len: usize, i: usize) T {
            std.debug.assert(i < len and len <= arr.len);
            const val = arr[i];
            std.mem.copyForwards(T, arr[i .. len - 1], arr[i + 1 .. len]);
            arr[len - 1] = undefined;
            return val;
        }

        const IndexResult = union(enum) {
            /// the key is in `keys[i]` and its value is in `vals[i]`
            found: usize,
            /// the key and value are in a sub-tree at `ptrs[i]`
            not_found: usize,
        };

        fn indexOf(key: K, keys: []const K) IndexResult {
            var idx: usize = undefined;
            if (cfg.search == .binary) {
                idx = std.sort.lowerBound(
                    K,
                    keys,
                    key,
                    cfg.cmp,
                );
            } else {
                idx = 0;
                while (idx < keys.len and cfg.cmp(keys[idx], key) == .lt) : (idx += 1) {}
            }

            if (idx >= keys.len or keys[idx] != key) {
                return .{ .not_found = idx };
            } else {
                return .{ .found = idx };
            }
        }

        pub fn put(
            self: *Self,
            alloc: std.mem.Allocator,
            key: K,
            val: V,
        ) Error!void {
            _ = try self.fetchPut(alloc, key, val);
        }

        /// insert `val` at `key`, returning the old value
        pub fn fetchPut(
            self: *Self,
            alloc: std.mem.Allocator,
            key: K,
            val: V,
        ) Error!?KV {
            // lazy init, happens only once
            if (self.root == null) {
                @branchHint(.cold);
                const node: *LeafNode = try alloc.create(LeafNode);
                node.* = .{};
                node.keys[0] = key;
                node.vals[0] = val;
                node.used = 1;

                self.root = @ptrCast(node);
                self.size += 1;
                return null;
            }

            // split full nodes pre-emptitively
            const root_node = getNode(self.root.?, self.depth);
            if (root_node.used.* == root_node.max) try self.splitRoot(alloc);

            var root = self.root.?;
            var depth = self.depth;

            while (true) : (depth -= 1) {
                const node = getNode(root, depth);
                std.debug.assert(node.used.* != node.max);

                // replace if the slot is already in use
                var i = switch (indexOf(key, node.keys[0..node.used.*])) {
                    .found => |i| {
                        const old: KV = .{ .key = node.keys[i], .value = node.vals[i] };
                        node.keys[i] = key;
                        node.vals[i] = val;
                        return old;
                    },
                    .not_found => |i| i,
                };

                // if its a leaf node: insert
                // if its a branch node: continue
                if (depth == 0) {
                    insertArr(K, node.keys[0..], node.used.*, i, key);
                    insertArr(V, node.vals[0..], node.used.*, i, val);
                    node.used.* += 1;
                    self.size += 1;
                    return null;
                } else {
                    // split full nodes pre-emptitively
                    if (isMaxCapacity(node.ptrs[i], depth - 1)) {
                        try splitNthChild(alloc, root, depth, i);
                        switch (cfg.cmp(node.keys[i], key)) {
                            .lt => i += 1,
                            .eq => {
                                const old: KV = .{ .key = node.keys[i], .value = node.vals[i] };
                                node.keys[i] = key;
                                node.vals[i] = val;
                                return old;
                            },
                            .gt => {},
                        }
                    }

                    root = node.ptrs[i];
                }
            }
        }

        pub fn fetchRemove(self: *Self, alloc: std.mem.Allocator, key: K) ?KV {
            return self.removeNode(
                alloc,
                self.root orelse return null,
                self.depth,
                key,
            );
        }

        fn removeNode(
            self: *Self,
            alloc: std.mem.Allocator,
            node_ptr: *AnyNode,
            depth: usize,
            wanted_key: K,
        ) ?KV {
            if (depth == 0) {
                const node: *LeafNode = @ptrCast(@alignCast(node_ptr));

                // only the root can have less than MIN keys,
                // this operation will remove one key from the parent
                std.debug.assert(node.used > LeafNode.MIN or node_ptr == self.root);

                switch (indexOf(wanted_key, node.keys[0..node.used])) {
                    .found => |i| {
                        // root can have less than min keys
                        const key = removeArr(K, node.keys[0..], node.used, i);
                        const val = removeArr(V, node.vals[0..], node.used, i);
                        node.used -= 1;
                        self.size -= 1;
                        std.debug.assert(wanted_key == key);
                        return .{ .key = key, .value = val };
                    },
                    .not_found => {
                        return null;
                    },
                }
            }

            const node: *BranchNode = @ptrCast(@alignCast(node_ptr));

            // only the root can have less than MIN keys,
            // this operation will remove one key from the parent
            std.debug.assert(node.used > BranchNode.MIN or node_ptr == self.root);

            switch (indexOf(wanted_key, node.keys[0..node.used])) {
                .found => |i| {
                    self.size -= 1;
                    return self.removeBranch(alloc, node, depth, i);
                },
                .not_found => |i| {
                    var next_node_ptr = node.ptrs[i];
                    var next_depth = depth - 1;

                    var next_node = getNode(next_node_ptr, next_depth);
                    if (next_node.used.* == next_node.min) {
                        // pre-emptitively merge nodes
                        if (i != 0 and i != node.used) {
                            stealFromPrevSibling(node, depth, i) catch {
                                stealFromNextSibling(node, depth, i) catch {
                                    self.mergeNthChildWithNext(
                                        alloc,
                                        node,
                                        depth,
                                        i,
                                    ) catch {
                                        next_node_ptr = self.root.?;
                                        next_depth = self.depth;
                                    };
                                };
                            };
                        } else if (i == 0) {
                            stealFromNextSibling(node, depth, i) catch {
                                self.mergeNthChildWithNext(
                                    alloc,
                                    node,
                                    depth,
                                    i,
                                ) catch {
                                    next_node_ptr = self.root.?;
                                    next_depth = self.depth;
                                };
                            };
                        } else if (i == node.used) {
                            stealFromPrevSibling(node, depth, i) catch {
                                if (self.mergeNthChildWithNext(
                                    alloc,
                                    node,
                                    depth,
                                    i - 1,
                                )) |_| {
                                    next_node_ptr = node.ptrs[i - 1];
                                } else |_| {
                                    next_node_ptr = self.root.?;
                                    next_depth = self.depth;
                                }
                            };
                        } else {
                            unreachable;
                        }

                        // if (i != 0 and i != node.used) {
                        //                             std.debug.print("case 1\n", .{});
                        //                             stealFromPrevSibling(node, depth, i) catch {
                        //                                 stealFromNextSibling(node, depth, i) catch {
                        //                                     self.mergeNthChildWithNext(
                        //                                         alloc,
                        //                                         node,
                        //                                         depth,
                        //                                         i,
                        //                                     ) catch {};
                        //                                 };
                        //                             };
                        //                         } else if (i == 0) {
                        //                             std.debug.print("case 2\n", .{});
                        //                             stealFromNextSibling(node, depth, i) catch {
                        //                                 self.mergeNthChildWithNext(
                        //                                     alloc,
                        //                                     node,
                        //                                     depth,
                        //                                     i,
                        //                                 ) catch {};
                        //                             };
                        //                         } else if (i == node.used) {
                        //                             std.debug.print("case 3\n", .{});
                        //                             stealFromPrevSibling(node, depth, i) catch {
                        //                                 self.mergeNthChildWithNext(
                        //                                     alloc,
                        //                                     node,
                        //                                     depth,
                        //                                     i - 1,
                        //                                 ) catch {};
                        //                                 next_node_ptr = node.ptrs[i - 1];
                        //                                 self.debug();
                        //                             };
                        //                         } else {
                        //                             std.debug.print("case 4? i={} used={}\n", .{ i, node.used });
                        //                             unreachable;
                        //                         }
                    }

                    next_node = getNode(next_node_ptr, next_depth);
                    std.debug.assert(next_node.used.* > next_node.min);

                    return self.removeNode(
                        alloc,
                        next_node_ptr,
                        next_depth,
                        wanted_key,
                    );
                },
            }
        }

        fn removeBranch(
            self: *Self,
            alloc: std.mem.Allocator,
            node: *BranchNode,
            depth: usize,
            found_index: usize,
        ) KV {
            const kv: KV = .{
                .key = node.keys[found_index],
                .value = node.vals[found_index],
            };

            switch (depth == 1) {
                inline else => |child_is_leaf| {
                    const ChildNode = if (child_is_leaf) LeafNode else BranchNode;

                    const lhs: *ChildNode = @ptrCast(@alignCast(node.ptrs[found_index]));
                    const rhs: *ChildNode = @ptrCast(@alignCast(node.ptrs[found_index + 1]));

                    if (lhs.used > ChildNode.MIN) {
                        const pred = self.removePredecessor(alloc, lhs, depth - 1);
                        node.keys[found_index] = pred.key;
                        node.vals[found_index] = pred.value;
                        return kv;
                    } else if (rhs.used > ChildNode.MIN) {
                        const succ = self.removeSuccessor(alloc, rhs, depth - 1);
                        node.keys[found_index] = succ.key;
                        node.vals[found_index] = succ.value;
                        return kv;
                    }

                    self.mergeNthChildWithNext(
                        alloc,
                        node,
                        depth,
                        found_index,
                    ) catch {};
                    if (ChildNode == LeafNode) {
                        const key = removeArr(K, lhs.keys[0..], lhs.used, ChildNode.MIN);
                        const val = removeArr(V, lhs.vals[0..], lhs.used, ChildNode.MIN);
                        lhs.used -= 1;
                        std.debug.assert(key == kv.key);
                        std.debug.assert(val == kv.value);
                        return kv;
                    } else {
                        return self.removeBranch(alloc, lhs, depth - 1, ChildNode.MIN);
                    }
                },
            }
        }

        fn removePredecessor(
            self: *Self,
            alloc: std.mem.Allocator,
            node_ptr: *AnyNode,
            depth: usize,
        ) KV {
            if (depth == 0) {
                const node: *LeafNode = @ptrCast(@alignCast(node_ptr));
                std.debug.assert(node.used > LeafNode.MIN);

                const key = removeArr(K, node.keys[0..], node.used, node.used - 1);
                const val = removeArr(V, node.vals[0..], node.used, node.used - 1);
                node.used -= 1;
                return .{ .key = key, .value = val };
            } else {
                const node: *BranchNode = @ptrCast(@alignCast(node_ptr));
                std.debug.assert(node.used > BranchNode.MIN);

                var last = node.used;
                const child_size = nodeSize(node.ptrs[last], depth - 1);
                const sibling_size = nodeSize(node.ptrs[last - 1], depth - 1);
                if (child_size == .min and sibling_size == .min) {
                    last -= 1;
                    self.mergeNthChildWithNext(
                        alloc,
                        node,
                        depth,
                        last,
                    ) catch unreachable; // should already be checked in previous levels
                } else if (child_size == .min) {
                    stealFromPrevSibling(
                        node,
                        depth,
                        last,
                    ) catch {};
                }

                return self.removePredecessor(alloc, node.ptrs[last], depth - 1);
            }
        }

        fn removeSuccessor(
            self: *Self,
            alloc: std.mem.Allocator,
            node_ptr: *AnyNode,
            depth: usize,
        ) KV {
            if (depth == 0) {
                const node: *LeafNode = @ptrCast(@alignCast(node_ptr));
                std.debug.assert(node.used > LeafNode.MIN);

                const key = removeArr(K, node.keys[0..], node.used, 0);
                const val = removeArr(V, node.vals[0..], node.used, 0);
                node.used -= 1;
                return .{ .key = key, .value = val };
            } else {
                const node: *BranchNode = @ptrCast(@alignCast(node_ptr));
                std.debug.assert(node.used > BranchNode.MIN);

                const first = 0;
                const child_size = nodeSize(node.ptrs[first], depth - 1);
                const sibling_size = nodeSize(node.ptrs[first + 1], depth - 1);
                if (child_size == .min and sibling_size == .min) {
                    self.mergeNthChildWithNext(
                        alloc,
                        node,
                        depth,
                        first,
                    ) catch unreachable; // should already be checked in previous levels
                } else if (child_size == .min) {
                    stealFromNextSibling(
                        node,
                        depth,
                        first,
                    ) catch {};
                }

                return self.removeSuccessor(alloc, node.ptrs[first], depth - 1);
            }
        }

        fn stealFromPrevSibling(
            parent: *BranchNode,
            depth: usize,
            n: usize,
        ) error{SiblingAtMinimum}!void {
            std.debug.assert(depth != 0);

            switch (depth == 1) {
                inline else => |child_is_leaf| {
                    const ChildNode = if (child_is_leaf) LeafNode else BranchNode;

                    const dst: *ChildNode = @ptrCast(@alignCast(parent.ptrs[n]));
                    const src: *ChildNode = @ptrCast(@alignCast(parent.ptrs[n - 1]));

                    if (src.used == ChildNode.MIN)
                        return error.SiblingAtMinimum;

                    std.debug.assert(dst.used != ChildNode.MAX);
                    std.debug.assert(src.used != ChildNode.MIN);

                    insertArr(K, dst.keys[0..], dst.used, 0, parent.keys[n - 1]);
                    parent.keys[n - 1] = src.keys[src.used - 1];
                    src.keys[src.used - 1] = undefined;

                    insertArr(V, dst.vals[0..], dst.used, 0, parent.vals[n - 1]);
                    parent.vals[n - 1] = src.vals[src.used - 1];
                    src.vals[src.used - 1] = undefined;

                    if (!child_is_leaf) {
                        insertArr(*AnyNode, dst.ptrs[0..], dst.used + 1, 0, src.ptrs[src.used]);
                        src.ptrs[src.used] = undefined;
                    }

                    dst.used += 1;
                    src.used -= 1;
                },
            }
        }

        fn stealFromNextSibling(
            parent: *BranchNode,
            depth: usize,
            n: usize,
        ) error{SiblingAtMinimum}!void {
            std.debug.assert(depth != 0);

            switch (depth == 1) {
                inline else => |child_is_leaf| {
                    const ChildNode = if (child_is_leaf) LeafNode else BranchNode;

                    const dst: *ChildNode = @ptrCast(@alignCast(parent.ptrs[n]));
                    const src: *ChildNode = @ptrCast(@alignCast(parent.ptrs[n + 1]));

                    if (src.used == ChildNode.MIN)
                        return error.SiblingAtMinimum;

                    std.debug.assert(dst.used != ChildNode.MAX);
                    std.debug.assert(src.used != ChildNode.MIN);

                    dst.keys[dst.used] = parent.keys[n];
                    parent.keys[n] = removeArr(K, src.keys[0..], src.used, 0);

                    dst.vals[dst.used] = parent.vals[n];
                    parent.vals[n] = removeArr(V, src.vals[0..], src.used, 0);

                    if (!child_is_leaf) {
                        dst.ptrs[dst.used + 1] = removeArr(*AnyNode, src.ptrs[0..], src.used + 1, 0);
                    }

                    dst.used += 1;
                    src.used -= 1;
                },
            }
        }

        fn mergeNthChildWithNext(
            self: *Self,
            alloc: std.mem.Allocator,
            parent: *BranchNode,
            depth: usize,
            n: usize,
        ) error{NewRootCreated}!void {
            std.debug.assert(depth != 0);

            // only the root can have less than MIN keys,
            // this operation will remove one key from the parent
            std.debug.assert(parent.used > BranchNode.MIN or @intFromPtr(self.root) == @intFromPtr(parent));

            const median_key = removeArr(K, parent.keys[0..], parent.used, n);
            const median_val = removeArr(V, parent.vals[0..], parent.used, n);
            const src_node = removeArr(*AnyNode, parent.ptrs[0..], parent.used + 1, n + 1);
            parent.used -= 1;

            switch (depth == 1) {
                inline else => |child_is_leaf| {
                    const ChildNode = if (child_is_leaf) LeafNode else BranchNode;

                    const dst: *ChildNode = @ptrCast(@alignCast(parent.ptrs[n]));
                    const src: *ChildNode = @ptrCast(@alignCast(src_node));

                    std.debug.assert(dst != src);
                    std.debug.assert(dst.used == ChildNode.MIN);
                    std.debug.assert(src.used == ChildNode.MIN);

                    dst.keys = (dst.keys[0..ChildNode.MIN] ++ [1]K{median_key} ++ src.keys[0..ChildNode.MIN]).*;
                    dst.vals = (dst.vals[0..ChildNode.MIN] ++ [1]V{median_val} ++ src.vals[0..ChildNode.MIN]).*;
                    if (ChildNode == BranchNode) {
                        dst.ptrs = (dst.ptrs[0 .. ChildNode.MIN + 1] ++ src.ptrs[0 .. ChildNode.MIN + 1]).*;
                    }
                    dst.used = ChildNode.MAX;

                    alloc.destroy(src);
                    if (parent.used == 0) {
                        std.debug.assert(@intFromPtr(self.root) == @intFromPtr(parent));
                        self.root = dst;
                        self.depth -= 1;
                        alloc.destroy(parent);
                        return error.NewRootCreated;
                    }
                },
            }
        }

        pub fn debug(self: *const Self) void {
            std.debug.print("tree:\n", .{});
            self.debugRecurse(self.root, self.depth);
        }

        fn debugRecurse(self: *const Self, node_ptr: ?*AnyNode, depth: usize) void {
            const node = getNode(node_ptr orelse return, depth);

            if (depth == 0) {
                for (0..node.used.*) |i| {
                    std.debug.print("d={: <5}", .{depth});
                    debugIndent(self.depth - depth);
                    std.debug.print("|{}\n", .{node.keys[i]});
                }
            } else {
                for (0..node.used.*) |i| {
                    self.debugRecurse(node.ptrs[i], depth - 1);

                    std.debug.print("d={: <5}", .{depth});
                    debugIndent(self.depth - depth);
                    std.debug.print("|{}\n", .{node.keys[i]});
                }
                self.debugRecurse(node.ptrs[node.used.*], depth - 1);
            }
        }

        fn debugIndent(depth: usize) void {
            for (0..depth) |_| {
                std.debug.print("    ", .{});
            }
        }

        pub fn get(self: *const Self, key: K) ?V {
            return (self.getPtr(key) orelse return null).*;
        }

        pub fn getPtr(self: *const Self, key: K) ?*V {
            var root = self.root;
            var depth = self.depth;

            while (true) : (depth -= 1) {
                const node = getNode(root orelse return null, depth);

                const i = switch (indexOf(key, node.keys[0..node.used.*])) {
                    .found => |i| return &node.vals[i],
                    .not_found => |i| i,
                };

                if (depth == 0) {
                    return null;
                } else {
                    root = node.ptrs[i];
                }
            }
        }

        pub fn deinit(self: *Self, alloc: std.mem.Allocator) void {
            self.clear(alloc);
            self.* = undefined;
        }

        pub fn clear(self: *Self, alloc: std.mem.Allocator) void {
            clearRecurse(alloc, self.root, self.depth);
            self.* = .{};
        }

        fn clearRecurse(
            alloc: std.mem.Allocator,
            node_ptr: ?*AnyNode,
            depth: usize,
        ) void {
            if (node_ptr == null) return;
            if (depth != 0) {
                const node: *BranchNode = @ptrCast(@alignCast(node_ptr));
                for (node.ptrs[0 .. node.used + 1]) |next| {
                    clearRecurse(alloc, next, depth - 1);
                }
                alloc.destroy(node);
            } else {
                const node: *LeafNode = @ptrCast(@alignCast(node_ptr));
                alloc.destroy(node);
            }
        }

        pub fn forEach(
            self: *const Self,
            ctx: anytype,
            f: fn (@TypeOf(ctx), K, V) void,
        ) void {
            forEachRecurse(ctx, f, self.root, self.depth);
        }

        fn forEachRecurse(
            ctx: anytype,
            f: fn (@TypeOf(ctx), K, V) void,
            node_ptr: ?*const AnyNode,
            depth: usize,
        ) void {
            if (node_ptr == null) return;
            if (depth != 0) {
                const node: *const BranchNode = @ptrCast(@alignCast(node_ptr));
                for (node.keys[0..node.used], node.vals[0..node.used]) |k, v| {
                    f(ctx, k, v);
                }
                for (node.ptrs[0 .. node.used + 1]) |next| {
                    forEachRecurse(ctx, f, next, depth - 1);
                }
            } else {
                const node: *const LeafNode = @ptrCast(@alignCast(node_ptr));
                for (node.keys[0..node.used], node.vals[0..node.used]) |k, v| {
                    f(ctx, k, v);
                }
            }
        }

        pub fn verify(self: *const Self) void {
            const root = self.root orelse return;

            var last: ?*const K = null;
            self.verifyOrderRecurse(root, self.depth, &last);
            self.verifyLimitsRecurse(root, self.depth);
        }

        fn verifyLimitsRecurse(
            self: *const Self,
            node_ptr: *const AnyNode,
            depth: usize,
        ) void {
            if (depth != 0) {
                const node: *const BranchNode = @ptrCast(@alignCast(node_ptr));
                if (node_ptr != self.root)
                    std.debug.assert(BranchNode.MIN <= node.used);
                std.debug.assert(node.used <= BranchNode.MAX);

                for (node.ptrs[0 .. node.used + 1]) |next| {
                    self.verifyLimitsRecurse(next, depth - 1);
                }
            } else {
                const node: *const LeafNode = @ptrCast(@alignCast(node_ptr));
                if (node_ptr != self.root)
                    std.debug.assert(LeafNode.MIN <= node.used);
                std.debug.assert(node.used <= LeafNode.MAX);
            }
        }

        fn verifyOrderRecurse(
            self: *const Self,
            node_ptr: ?*const AnyNode,
            depth: usize,
            last_item: *?*const K,
        ) void {
            if (depth != 0) {
                const node: *const BranchNode = @ptrCast(@alignCast(node_ptr));

                for (0..node.used) |i| {
                    self.verifyOrderRecurse(node.ptrs[i], depth - 1, last_item);

                    const cur = &node.keys[i];
                    if (last_item.*) |prev|
                        std.debug.assert(cfg.cmp(prev.*, cur.*) == .lt);
                    last_item.* = cur;
                }
                self.verifyOrderRecurse(node.ptrs[node.used], depth - 1, last_item);
            } else {
                const node: *const LeafNode = @ptrCast(@alignCast(node_ptr));

                for (0..node.used) |i| {
                    const cur = &node.keys[i];
                    if (last_item.*) |prev|
                        std.debug.assert(cfg.cmp(prev.*, cur.*) == .lt);
                    last_item.* = cur;
                }
            }
        }

        fn nodeSize(node_ptr: *const AnyNode, depth: usize) enum { min, mid, max } {
            switch (depth == 0) {
                inline else => |is_leaf| {
                    const T = if (is_leaf) LeafNode else BranchNode;
                    const node: *const T = @ptrCast(@alignCast(node_ptr));

                    std.debug.assert(node.used >= T.MIN);
                    std.debug.assert(node.used <= T.MAX);

                    return if (node.used == T.MIN)
                        .min
                    else if (node.used == T.MAX)
                        .max
                    else
                        .mid;
                },
            }
        }

        fn isMaxCapacity(node_ptr: *const AnyNode, depth: usize) bool {
            if (depth == 0) {
                const node: *const LeafNode = @ptrCast(@alignCast(node_ptr));
                return node.used == LeafNode.MAX;
            } else {
                const node: *const BranchNode = @ptrCast(@alignCast(node_ptr));
                return node.used == BranchNode.MAX;
            }
        }

        fn isMinCapacity(node_ptr: *const AnyNode, depth: usize) bool {
            if (depth == 0) {
                const node: *const LeafNode = @ptrCast(@alignCast(node_ptr));
                return node.used == LeafNode.MIN;
            } else {
                const node: *const BranchNode = @ptrCast(@alignCast(node_ptr));
                return node.used == BranchNode.MIN;
            }
        }

        fn splitNthChild(alloc: std.mem.Allocator, node_ptr: *AnyNode, depth: usize, n: usize) Error!void {
            std.debug.assert(depth != 0);

            const parent: *BranchNode = @ptrCast(@alignCast(node_ptr));
            std.debug.assert(parent.used != BranchNode.MAX);

            const full_node = getNode(parent.ptrs[n], depth - 1);
            std.debug.assert(full_node.used.* == full_node.max);

            var new_node: Node = undefined;
            if (depth == 1) {
                const node = try alloc.create(LeafNode);
                node.* = .{};
                new_node = Node.fromLeaf(node);
            } else {
                const node = try alloc.create(BranchNode);
                node.* = .{};
                new_node = Node.fromBranch(node);
            }

            // copy the right half (maybe less than the minimum, so it becomes invalid) to the new node
            std.mem.copyForwards(K, new_node.keys[0..], full_node.keys[full_node.min + 1 ..]);
            std.mem.copyForwards(V, new_node.vals[0..], full_node.vals[full_node.min + 1 ..]);
            if (depth != 1) // move the children also if its a branch node
                std.mem.copyForwards(*anyopaque, new_node.ptrs[0..], full_node.ptrs[full_node.min + 1 ..]);
            // move the first one from the right half to the parent
            insertArr(K, parent.keys[0..], parent.used, n, full_node.keys[full_node.min]);
            insertArr(V, parent.vals[0..], parent.used, n, full_node.vals[full_node.min]);
            // add the new child
            insertArr(*anyopaque, parent.ptrs[0..], parent.used + 1, n + 1, new_node.ptr);

            full_node.used.* = full_node.min;
            new_node.used.* = full_node.min;
            parent.used += 1;
        }

        fn splitRoot(self: *Self, alloc: std.mem.Allocator) Error!void {
            const new_root = try alloc.create(BranchNode);
            new_root.* = .{};
            new_root.ptrs[0] = self.root.?;

            self.root = @ptrCast(new_root);
            self.depth += 1;

            // FIXME: recover from errors
            try splitNthChild(alloc, @ptrCast(new_root), self.depth, 0);
        }
    };
}

// test cases stolen from Rust's BTreeMap doctests

test "insert" {
    var map: BTreeMap(usize, u8, .{}) = .{};
    defer map.clear(std.testing.allocator);

    map.verify();
    try std.testing.expectEqual(0, map.size);

    var old = try map.fetchPut(std.testing.allocator, 1, 'a');
    try std.testing.expectEqual(null, old);

    map.verify();
    try std.testing.expectEqual(1, map.size);

    old = try map.fetchPut(std.testing.allocator, 1, 'b');
    try std.testing.expectEqual('a', old.?.value);

    map.verify();
    try std.testing.expectEqual(1, map.size);
}

test "remove" {
    var map: BTreeMap(usize, u8, .{}) = .{};
    defer map.clear(std.testing.allocator);

    map.verify();
    try std.testing.expectEqual(0, map.size);

    const old = try map.fetchPut(std.testing.allocator, 1, 'a');
    try std.testing.expectEqual(null, old);

    map.verify();
    try std.testing.expectEqual(1, map.size);

    var removed = map.fetchRemove(std.testing.allocator, 1);
    try std.testing.expectEqual(1, removed.?.key);
    try std.testing.expectEqual('a', removed.?.value);

    map.verify();
    try std.testing.expectEqual(0, map.size);

    removed = map.fetchRemove(std.testing.allocator, 1);
    try std.testing.expectEqual(null, removed);

    map.verify();
    try std.testing.expectEqual(0, map.size);
}

test "clear" {
    var map: BTreeMap(usize, u8, .{}) = .{};
    defer map.clear(std.testing.allocator);

    map.verify();
    try std.testing.expectEqual(0, map.size);

    const old = try map.fetchPut(std.testing.allocator, 1, 'a');
    try std.testing.expectEqual(null, old);

    map.verify();
    try std.testing.expectEqual(1, map.size);

    map.clear(std.testing.allocator);
    try std.testing.expectEqual(0, map.size);

    map.verify();
    try std.testing.expectEqual(0, map.size);
}

test "get" {
    var map: BTreeMap(usize, u8, .{}) = .{};
    defer map.clear(std.testing.allocator);

    const old = try map.fetchPut(std.testing.allocator, 1, 'a');

    try std.testing.expectEqual(null, old);
    try std.testing.expectEqual('a', map.get(1));
    try std.testing.expectEqual(null, map.get(2));
}

test "forEach" {
    var map: BTreeMap(usize, *u8, .{}) = .{};
    defer map.clear(std.testing.allocator);

    const val1 = try std.testing.allocator.create(u8);
    val1.* = 'a';
    const val2 = try std.testing.allocator.create(u8);
    val2.* = 'a';

    var old = try map.fetchPut(std.testing.allocator, 1, val1);
    try std.testing.expectEqual(null, old);
    old = try map.fetchPut(std.testing.allocator, 2, val2);
    try std.testing.expectEqual(null, old);

    // TODO: iterator
    map.forEach({}, struct {
        fn f(_: void, _: usize, v: *u8) void {
            std.testing.allocator.destroy(v);
        }
    }.f);
}

test "insert multiple remove multiple" {
    var map: BTreeMap(usize, u8, .{}) = .{};
    defer map.clear(std.testing.allocator);

    map.verify();
    try std.testing.expectEqual(0, map.size);

    var old = try map.fetchPut(std.testing.allocator, 1, 'a');
    try std.testing.expectEqual(null, old);

    map.verify();
    try std.testing.expectEqual(1, map.size);

    old = try map.fetchPut(std.testing.allocator, 4, 'd');
    try std.testing.expectEqual(null, old);

    map.verify();
    try std.testing.expectEqual(2, map.size);

    old = try map.fetchPut(std.testing.allocator, 3, 'c');
    try std.testing.expectEqual(null, old);

    map.verify();
    try std.testing.expectEqual(3, map.size);

    old = try map.fetchPut(std.testing.allocator, 2, 'b');
    try std.testing.expectEqual(null, old);

    map.verify();
    try std.testing.expectEqual(4, map.size);

    var kv = map.fetchRemove(std.testing.allocator, 4);
    try std.testing.expectEqual(4, kv.?.key);
    try std.testing.expectEqual('d', kv.?.value);

    map.verify();
    try std.testing.expectEqual(3, map.size);

    kv = map.fetchRemove(std.testing.allocator, 3);
    try std.testing.expectEqual(3, kv.?.key);
    try std.testing.expectEqual('c', kv.?.value);

    map.verify();
    try std.testing.expectEqual(2, map.size);

    kv = map.fetchRemove(std.testing.allocator, 3);
    try std.testing.expectEqual(null, kv);

    map.verify();
    try std.testing.expectEqual(2, map.size);
}

fn expectKvEq(expected: anytype, actual: anytype) !void {
    if ((expected == null) != (actual == null))
        return error.TestExpectedEqual;

    if (expected == null) return;

    try std.testing.expectEqual(expected.?.key, actual.?.key);
    try std.testing.expectEqual(expected.?.value, actual.?.value);
}

test "fuzz" {
    const op_limit = 128;
    const key_limit = 64;

    try std.testing.fuzz({}, struct {
        fn testOne(_: void, _input: []const u8) anyerror!void {
            if (!@import("builtin").fuzz) return;

            var input = _input;

            const TreeMap = BTreeMap(usize, usize, .{
                .node_size = .{ .nodes = 5 },
            });
            const HashMap = std.AutoHashMapUnmanaged(usize, usize);

            var treemap: TreeMap = .{};
            defer treemap.deinit(std.testing.allocator);
            var hashmap: HashMap = .{};
            defer hashmap.deinit(std.testing.allocator);

            var ops_left: u8 = op_limit;

            std.debug.print("\ntreemap(leaf_min={}, leaf_max={}, branch_min={}, branch_max={})\n", .{
                TreeMap.LeafNode.MIN,
                TreeMap.LeafNode.MAX,
                TreeMap.BranchNode.MIN,
                TreeMap.BranchNode.MAX,
            });

            while (true) {
                if (ops_left == 0) break;
                ops_left -= 1;

                if (input.len < 1) break;
                const opcode: u8 = input[0];
                input = input[1..];

                if (input.len < 1) break;
                const key = std.mem.readInt(u8, input[0..1], .little) % key_limit;
                input = input[1..];

                switch (@as(u2, @truncate(opcode % 4))) {
                    0 => {
                        if (input.len < 1) break;
                        const val = std.mem.readInt(u8, input[0..1], .little);
                        input = input[1..];

                        std.debug.print("fetchPut(key={}, val={}, size={}, depth={})\n", .{
                            key,
                            val,
                            treemap.size,
                            treemap.depth,
                        });

                        const v1 = try treemap.fetchPut(std.testing.allocator, key, val);
                        const v2 = try hashmap.fetchPut(std.testing.allocator, key, val);
                        treemap.debug();
                        try expectKvEq(v1, v2);
                    },
                    1 => {
                        std.debug.print("fetchRemove(key={}, size={}, depth={})\n", .{
                            key,
                            treemap.size,
                            treemap.depth,
                        });

                        const v1 = treemap.fetchRemove(std.testing.allocator, key);
                        const v2 = hashmap.fetchRemove(key);
                        treemap.debug();
                        try expectKvEq(v1, v2);
                    },
                    2 => {
                        std.debug.print("get(key={}, size={}, depth={})\n", .{
                            key,
                            treemap.size,
                            treemap.depth,
                        });

                        const v1 = treemap.get(key);
                        const v2 = hashmap.get(key);
                        treemap.debug();
                        try std.testing.expectEqual(v2, v1);
                    },
                    3 => {
                        std.debug.print("clear(size={}, depth={})\n", .{
                            treemap.size,
                            treemap.depth,
                        });

                        treemap.clear(std.testing.allocator);
                        hashmap.clearRetainingCapacity();
                        treemap.debug();
                    },
                }

                treemap.verify();
                try std.testing.expectEqual(hashmap.size, treemap.size);
            }

            try std.testing.expectEqual(hashmap.count(), treemap.size);
        }
    }.testOne, .{});
}
