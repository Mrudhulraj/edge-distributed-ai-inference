from __future__ import annotations

import logging
import os
import threading
import time
from concurrent import futures
from typing import List, Sequence

import grpc
import redis

import users_pb2
import users_pb2_grpc


def _build_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("node-app")


LOGGER = _build_logger()


class RedisUserStore:
    def __init__(self, host: str, port: int, password: str | None = None):
        self._client = redis.Redis(
            host=host,
            port=port,
            password=password,
            decode_responses=True,
        )

    def list_users(self) -> List[users_pb2.User]:
        users: List[users_pb2.User] = []
        for user_id in sorted(self._client.smembers("users:ids")):
            data = self._client.hgetall(self._user_key(user_id))
            if not data:
                continue
            users.append(
                users_pb2.User(
                    id=user_id,
                    name=data.get("name", ""),
                    email=data.get("email", ""),
                )
            )
        return users

    def get_user(self, user_id: str) -> users_pb2.User | None:
        data = self._client.hgetall(self._user_key(user_id))
        if not data:
            return None
        return users_pb2.User(
            id=user_id,
            name=data.get("name", ""),
            email=data.get("email", ""),
        )

    def upsert_user(self, user: users_pb2.User) -> None:
        pipe = self._client.pipeline()
        pipe.hset(
            self._user_key(user.id),
            mapping={"name": user.name, "email": user.email},
        )
        pipe.sadd("users:ids", user.id)
        pipe.execute()

    def delete_user(self, user_id: str) -> users_pb2.User | None:
        user = self.get_user(user_id)
        if not user:
            return None
        pipe = self._client.pipeline()
        pipe.delete(self._user_key(user_id))
        pipe.srem("users:ids", user_id)
        pipe.execute()
        return user

    def bulk_upsert(self, users: Sequence[users_pb2.User]) -> None:
        for user in users:
            self.upsert_user(user)

    @staticmethod
    def _user_key(user_id: str) -> str:
        return f"user:{user_id}"


class PeerReplicator:
    def __init__(self, node_id: str, peer_endpoints: Sequence[str]):
        self._node_id = node_id
        self._peer_endpoints = [peer for peer in peer_endpoints if peer]

    def should_propagate(self, context: grpc.ServicerContext) -> bool:
        for metadata in context.invocation_metadata():
            if metadata.key == "x-origin-node":
                return False
        return True

    @property
    def peers(self) -> Sequence[str]:
        return list(self._peer_endpoints)

    def propagate_create(self, user: users_pb2.User) -> None:
        request = users_pb2.CreateUserRequest(user=user)
        self._broadcast(lambda stub, meta: stub.CreateUser(request, metadata=meta))

    def propagate_update(self, user: users_pb2.User) -> None:
        request = users_pb2.UpdateUserRequest(user=user)
        self._broadcast(lambda stub, meta: stub.UpdateUser(request, metadata=meta))

    def propagate_delete(self, user_id: str) -> None:
        request = users_pb2.DeleteUserRequest(id=user_id)
        self._broadcast(lambda stub, meta: stub.DeleteUser(request, metadata=meta))

    def sync_from_peer(self, peer: str) -> Sequence[users_pb2.User]:
        with grpc.insecure_channel(peer) as channel:
            stub = users_pb2_grpc.UsersStub(channel)
            response = stub.GetUsers(users_pb2.GetUsersRequest(), timeout=5)
            return response.users

    def _broadcast(self, rpc_invoker) -> None:
        if not self._peer_endpoints:
            return

        metadata = (("x-origin-node", self._node_id),)
        for peer in self._peer_endpoints:
            try:
                with grpc.insecure_channel(peer) as channel:
                    stub = users_pb2_grpc.UsersStub(channel)
                    rpc_invoker(stub, metadata)
                    LOGGER.debug("Replicated change to peer %s", peer)
            except grpc.RpcError as err:
                LOGGER.warning(
                    "Unable to propagate change to %s: %s", peer, err.details()
                )


class PeerSyncWorker:
    def __init__(
        self,
        replicator: PeerReplicator,
        store: RedisUserStore,
        sync_interval: int,
    ):
        self._replicator = replicator
        self._store = store
        self._interval = max(sync_interval, 1)
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        while True:
            for peer in self._replicator.peers:
                try:
                    users = self._replicator.sync_from_peer(peer)
                    self._store.bulk_upsert(users)
                    LOGGER.debug("Pulled %d users from %s", len(users), peer)
                except grpc.RpcError as err:
                    LOGGER.debug(
                        "Peer %s not reachable during sync: %s", peer, err.details()
                    )
                except Exception as exc:  # pragma: no cover
                    LOGGER.warning("Unexpected error syncing from %s: %s", peer, exc)
            time.sleep(self._interval)


class UsersService(users_pb2_grpc.UsersServicer):
    def __init__(self, store: RedisUserStore, replicator: PeerReplicator):
        self._store = store
        self._replicator = replicator

    def GetUsers(self, request, context):
        users = self._store.list_users()
        return users_pb2.GetUsersResponse(users=users)

    def GetUserById(self, request, context):
        user = self._store.get_user(request.id)
        if not user:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"User {request.id} not found")
            return users_pb2.GetUserByIdResponse()
        return users_pb2.GetUserByIdResponse(user=user)

    def CreateUser(self, request, context):
        user = request.user
        if not user.id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("User id is required")
            return users_pb2.CreateUserResponse()

        self._store.upsert_user(user)

        if self._replicator.should_propagate(context):
            self._replicator.propagate_create(user)

        return users_pb2.CreateUserResponse(user=user)

    def UpdateUser(self, request, context):
        if not request.user.id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("User id is required")
            return users_pb2.UpdateUserResponse()

        if not self._store.get_user(request.user.id):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"User {request.user.id} not found")
            return users_pb2.UpdateUserResponse()

        self._store.upsert_user(request.user)

        if self._replicator.should_propagate(context):
            self._replicator.propagate_update(request.user)

        return users_pb2.UpdateUserResponse(id=request.user.id)

    def DeleteUser(self, request, context):
        deleted = self._store.delete_user(request.id)
        if not deleted:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"User {request.id} not found")
            return users_pb2.DeleteUserResponse()

        if self._replicator.should_propagate(context):
            self._replicator.propagate_delete(request.id)

        return users_pb2.DeleteUserResponse(user=deleted)


def parse_peer_endpoints(raw_value: str) -> List[str]:
    peers = [peer.strip() for peer in raw_value.split(",") if peer.strip()]
    return peers


def main():
    node_id = os.getenv("NODE_ID", "node-0")
    redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.getenv("REDIS_PASSWORD")
    grpc_port = int(os.getenv("GRPC_PORT", "50051"))
    peer_endpoints = parse_peer_endpoints(os.getenv("PEER_ENDPOINTS", ""))
    sync_interval = int(os.getenv("PEER_SYNC_INTERVAL", "10"))

    store = RedisUserStore(redis_host, redis_port, redis_password)
    replicator = PeerReplicator(node_id=node_id, peer_endpoints=peer_endpoints)
    users_service = UsersService(store, replicator)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    users_pb2_grpc.add_UsersServicer_to_server(users_service, server)
    server.add_insecure_port(f"[::]:{grpc_port}")
    server.start()

    LOGGER.info(
        "Node %s ready on grpc://0.0.0.0:%d (peers=%s)",
        node_id,
        grpc_port,
        peer_endpoints or "none",
    )

    if peer_endpoints:
        PeerSyncWorker(replicator, store, sync_interval).start()

    server.wait_for_termination()


if __name__ == "__main__":
    main()

