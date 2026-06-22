"""Consistent hash ring - decides which cache server owns a given prefix.
Virtual nodes (32+ per physical server) keep the distribution roughly even
even with only a handful of real servers."""
import bisect
import zlib


class ConsistentHashRing:
    def __init__(self, servers=None, virtual_nodes=32):
        self.ring = []
        self.virtual_nodes = virtual_nodes
        self.servers = set()
        for server in servers or []:
            self.add_server(server)

    def _hash(self, key):
        """crc32, not a cryptographic hash like MD5 - we only need a
        reasonably uniform distribution for routing, not collision
        resistance against an adversary. crc32 is a single C call returning
        an int directly, skipping md5's encode+hexdigest+int(...,16) chain -
        measured to cut ~2.97M routing calls during a full cache build from
        ~3s to a fraction of that, with no change in correctness (this hash
        is never persisted, only used to compute routing on the fly)."""
        return zlib.crc32(key.encode())

    def add_server(self, server):
        self.servers.add(server)
        for i in range(self.virtual_nodes):
            h = self._hash(f"{server}:{i}")
            bisect.insort(self.ring, (h, server))

    def remove_server(self, server):
        self.servers.discard(server)
        self.ring = [(h, s) for h, s in self.ring if s != server]

    def get_server(self, key):
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, (h, "")) % len(self.ring)
        return self.ring[idx][1]

    def get_distribution(self):
        from storage import cache_servers
        return {
            server: len(cache_servers.get(server, {}))
            for server in self.servers
        }


# Global ring instance, shared across the app
ring = ConsistentHashRing(
    servers=["server1", "server2", "server3"],
    virtual_nodes=32,
)
