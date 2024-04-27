import ray
import math
import random
import threading

STORAGE_NODE_QUANTITY = 15
MAX_CHUNK_LENGTH = 3
DUPLICATES_QUANTITY = 3


@ray.remote
class StorageNode:
    def __init__(self, id):
        self.id = id
        self.artifacts = {}  # artifacts[artifact_name][chunk_num] = chunk_content
        self.chunk_quantity = 0
        self.isAlive = True
        self.name_node_ref = None

    def set_name_node_ref(self, name_node_ref):
        self.name_node_ref = name_node_ref

    def get_chunk_quantity(self):
        return self.chunk_quantity

    def get_id(self):
        return self.id

    def get_status(self):
        return self.isAlive

    def change_status(self):
        self.isAlive = not self.isAlive
        if not self.isAlive:
            self.name_node_ref.handle_node_down.remote(self.id)

    def store_artifact(self, artifact_name, chunk_num, chunk_content):
        if artifact_name not in self.artifacts:
            self.artifacts[artifact_name] = {}
        self.artifacts[artifact_name][chunk_num] = chunk_content
        self.chunk_quantity += 1

    def update_artifact(self, artifact_name, chunk_num, new_chunk_content):
        if artifact_name in self.artifacts and chunk_num in self.artifacts[artifact_name]:
            self.artifacts[artifact_name][chunk_num] = new_chunk_content

    def delete_artifact_chunk(self, artifact_name, chunk_num):
        if artifact_name in self.artifacts and chunk_num in self.artifacts[artifact_name]:
            del self.artifacts[artifact_name][chunk_num]
            if not self.artifacts[artifact_name]:
                del self.artifacts[artifact_name]
                self.chunk_quantity -= 1

    def get_artifact_chunk(self, artifact_name, chunk_num):
        if artifact_name in self.artifacts and chunk_num in self.artifacts[artifact_name]:
            return self.artifacts[artifact_name][chunk_num]

    def get_info(self):
        info = f"Storage Node ID: {self.id}\n"
        info += f"Is Alive: {self.isAlive}\n"
        info += f"Total number of artifacts stored: {len(self.artifacts)}\n"

        for artifact_name, chunks in self.artifacts.items():
            info += f"Artifact Name: {artifact_name}\n"
            info += f"Number of chunks: {len(chunks)}\n"
            info += "Chunks:\n"
            for chunk_num, chunk_content in chunks.items():
                info += f"\tChunk {chunk_num}: {chunk_content}\n"
        return info
    
    def clear(self):
        self.artifacts = {}
        self.chunk_quantity = 0


class Artifact:
    def __init__(self, name, chunk_quantity, storage_chunk_nodes):
        self.name = name
        self.storage_chunk_nodes = storage_chunk_nodes
        self.chunk_quantity = chunk_quantity

    def show(self):
        print(f"Artifact {self.name}; Divided on {self.chunk_quantity} chunks.")


@ray.remote
class NameNode:
    def __init__(self, storage_nodes):
        self.artifacts = {}
        self.storage_nodes = storage_nodes
        self.max_chunk_len = MAX_CHUNK_LENGTH
        self.duplicates_quantity = DUPLICATES_QUANTITY

    def get_storage_nodes(self):
        return self.storage_nodes

    def sort_storage_nodes(self):
        alive_storage_nodes = [node for node in self.storage_nodes if ray.get(node.get_status.remote())]
        chunk_nums = ray.get([node.get_chunk_quantity.remote() for node in alive_storage_nodes])
        sorted_indices = sorted(range(len(chunk_nums)), key=lambda i: chunk_nums[i])
        sorted_alive_nodes = [alive_storage_nodes[i] for i in sorted_indices]
        return sorted_alive_nodes

    def upload_artifact(self, name, content):
        chunk_size = min(self.max_chunk_len, len(content))
        chunk_quantity = math.ceil(len(content) / chunk_size)
        chunks = [content[i:i + chunk_size] for i in range(0, len(content), chunk_size)]

        storage_chunk_nodes = []

        for chunk_num, chunk_content in enumerate(chunks, start=0):
            sorted_storage_nodes = self.sort_storage_nodes()
            duplicates_count = min(self.duplicates_quantity, len(sorted_storage_nodes))
            one_chunk_storage_nodes = sorted_storage_nodes[:duplicates_count]

            storage_chunk_nodes_per_chunk = []

            for _ in range(self.duplicates_quantity):
                storage_node = one_chunk_storage_nodes.pop(0)
                storage_node.store_artifact.remote(name, chunk_num, chunk_content)
                storage_chunk_nodes_per_chunk.append(storage_node)
            storage_chunk_nodes.append(storage_chunk_nodes_per_chunk)

        artifact = Artifact(name, chunk_quantity, storage_chunk_nodes)
        self.artifacts[name] = artifact

    def update_artifact(self, artifact_name, new_content):
        if artifact_name not in self.artifacts:
            print(f"Artifact '{artifact_name}' does not exist.")
            return

        artifact = self.artifacts[artifact_name]
        old_chunk_quantity = artifact.chunk_quantity
        new_chunk_size = min(self.max_chunk_len, len(new_content))
        new_chunk_quantity = math.ceil(len(new_content) / new_chunk_size)
        new_chunks = [new_content[i:i + new_chunk_size] for i in range(0, len(new_content), new_chunk_size)]

        if new_chunk_quantity < old_chunk_quantity:
            for chunk_num in range(new_chunk_quantity, old_chunk_quantity):
                for storage_node in artifact.storage_chunk_nodes[chunk_num]:
                    storage_node.delete_artifact_chunk.remote(artifact_name, chunk_num)
            del artifact.storage_chunk_nodes[new_chunk_quantity:]

        for chunk_num in range(new_chunk_quantity):
            if chunk_num < old_chunk_quantity:
                for storage_node in artifact.storage_chunk_nodes[chunk_num]:
                    storage_node.update_artifact.remote(artifact_name, chunk_num, new_chunks[chunk_num])
            else:
                sorted_storage_nodes = self.sort_storage_nodes()
                one_chunk_storage_nodes = sorted_storage_nodes[:self.duplicates_quantity]

                storage_chunk_nodes_per_chunk = []

                for _ in range(self.duplicates_quantity):
                    storage_node = one_chunk_storage_nodes.pop(0)
                    storage_node.store_artifact.remote(artifact_name, chunk_num, new_chunks[chunk_num])
                    storage_chunk_nodes_per_chunk.append(storage_node)

                artifact.storage_chunk_nodes.append(storage_chunk_nodes_per_chunk)

        artifact.chunk_quantity = new_chunk_quantity
        print(f"Artifact '{artifact_name}' updated.")

    def delete_artifact(self, artifact_name):
        if artifact_name not in self.artifacts:
            print(f"Artifact '{artifact_name}' does not exist.")
        else:
            artifact = self.artifacts[artifact_name]
            for chunk_num, storage_chunk_nodes in enumerate(artifact.storage_chunk_nodes, start=0):
                for storage_node in storage_chunk_nodes:
                    storage_node.delete_artifact_chunk.remote(artifact_name, chunk_num)
            del self.artifacts[artifact_name]
            print(f"Artifact '{artifact_name}' deleted.")

    def get_artifact(self, artifact_name):
        if artifact_name not in self.artifacts:
            print(f"Artifact '{artifact_name}' does not exist.")
            return None

        artifact = self.artifacts[artifact_name]
        artifact_chunks = []

        for chunk_num in range(len(artifact.storage_chunk_nodes)):
            storage_nodes = [storage_node.get_artifact_chunk.remote(artifact_name, chunk_num) for storage_node in
                             artifact.storage_chunk_nodes[chunk_num]]
            ready_chunk, _ = ray.wait(storage_nodes, num_returns=1)
            chunk_content = ray.get(ready_chunk)[0]
            artifact_chunks.append(chunk_content)

        full_artifact = ''.join(artifact_chunks)
        return full_artifact

    def list_artifacts(self):
        if not self.artifacts:
            print("No artifacts found.")
        else:
            print("List of artifacts:")
            for artifact_name in self.artifacts:
                artifact_content = self.get_artifact(artifact_name)
                print(f"- Artifact '{artifact_name}': {artifact_content}")

    def handle_node_down(self, dead_node_id):
        for artifact_name, artifact in self.artifacts.items():
            for chunk_num, storage_chunk_nodes in enumerate(artifact.storage_chunk_nodes):
                if any(node.get_id.remote() == dead_node_id for node in storage_chunk_nodes):
                    for dead_node in storage_chunk_nodes:
                        if ray.get(dead_node.get_id.remote()) == dead_node_id:
                            storage_chunk_nodes.remove(dead_node)
                            artifact.storage_chunk_nodes[chunk_num] = storage_chunk_nodes
                            self.artifacts[artifact_name] = artifact
                            self.redistribute_chunks(artifact_name, chunk_num)
                            break
        self.clear_node(dead_node_id)

    def redistribute_chunks(self, artifact_name, chunk_num):
        alive_nodes = self.sort_storage_nodes()
        duplicate_chunks = set()

        for storage_node in alive_nodes:
            if artifact_name in storage_node.artifacts and chunk_num in storage_node.artifacts[artifact_name]:
                duplicate_chunks.add(storage_node)
                continue

            storage_node.store_artifact.remote(artifact_name, chunk_num, self.get_artifact(artifact_name))
            artifact = self.artifacts[artifact_name]
            artifact.storage_chunk_nodes[chunk_num].append(storage_node)
            self.artifacts[artifact_name] = artifact
            return

        if duplicate_chunks:
            first_duplicate_node = duplicate_chunks.pop()
            first_duplicate_node.store_artifact.remote(artifact_name, chunk_num, self.get_artifact(artifact_name))
            artifact = self.artifacts[artifact_name]
            artifact.storage_chunk_nodes[chunk_num].append(first_duplicate_node)
            self.artifacts[artifact_name] = artifact
            return
        
    def clear_node(self, dead_node_id):
        for node in self.storage_nodes:
            if node.get_id.remote() == dead_node_id:
                ray.get(node.clear.remote())
        
        
    def list_statuses(self):
        statuses = ray.get([storage_node.get_status.remote() for storage_node in self.storage_nodes])
        ids = ray.get([storage_node.get_id.remote() for storage_node in self.storage_nodes])

        for status, node_id in zip(statuses, ids):
            print(f": ID: {node_id} : {'Alive' if status else 'Dead'}")

    def get_storage_node_info(self, storage_node_id):
        node = self.storage_nodes[storage_node_id]
        return ray.get(node.get_info.remote())

    def list_storage_nodes(self):
        info = "List of storage nodes: \n"
        node_ids = ray.get([node.get_id.remote() for node in self.storage_nodes])
        node_chunk_num = ray.get([node.get_chunk_quantity.remote() for node in self.storage_nodes])
        for node_id in range(len(node_ids)):
            info += f"Storage Node ID: {node_ids[node_id]} ; Chunk quantity {node_chunk_num[node_id]}\n"
        return info


def upload_artifact(artifact_name, artifact_content):
    name_node = get_or_create_name_node()
    ray.get(name_node.upload_artifact.remote(artifact_name, artifact_content))


def update_artifact(artifact_name, new_content):
    name_node = get_or_create_name_node()
    ray.get(name_node.update_artifact.remote(artifact_name, new_content))


def delete_artifact(artifact_name):
    name_node = get_or_create_name_node()
    ray.get(name_node.delete_artifact.remote(artifact_name))


def get_artifact(artifact_name):
    name_node = get_or_create_name_node()
    return ray.get(name_node.get_artifact.remote(artifact_name))


def get_node_info(node_id):
    name_node = get_or_create_name_node()
    return ray.get(name_node.get_storage_node_info.remote(node_id))


def list_artifacts():
    name_node = get_or_create_name_node()
    ray.get(name_node.list_artifacts.remote())


def list_statuses():
    name_node = get_or_create_name_node()
    return ray.get(name_node.list_statuses.remote())


def list_nodes():
    name_node = get_or_create_name_node()
    return ray.get(name_node.list_storage_nodes.remote())


def get_or_create_name_node():
    global _name_node_instance
    if "_name_node_instance" not in globals():
        storage_nodes = [
            StorageNode.options(name=f"StorageNode_{id}", lifetime="detached", namespace="storage", get_if_exists=True).remote(id) for id in
            range(STORAGE_NODE_QUANTITY)]
        _name_node_instance = NameNode.options(name="MainNameNode", lifetime="detached", namespace="storage", get_if_exists=True).remote(
            storage_nodes)
        for node in ray.get(_name_node_instance.get_storage_nodes.remote()):
            node.set_name_node_ref.remote(_name_node_instance)
    return _name_node_instance


def main():
    if not ray.is_initialized():
        ray.init(address='ray://localhost:10001')
        # ray.init()
        print("Storage server starting...")

    global running, condition
    running = True
    condition = threading.Condition()

    def loop():
        global running
        deaths = 0
        name_node = get_or_create_name_node()
        storage_nodes = ray.get(name_node.get_storage_nodes.remote())
        while True:
            with condition:
                condition.wait(15)
                if not running:
                    break
                node_to_toggle = random.choice(storage_nodes)
                if ray.get(node_to_toggle.get_status.remote()):
                    if deaths < STORAGE_NODE_QUANTITY // 2:
                        ray.get(node_to_toggle.change_status.remote())
                        deaths += 1
                else:
                    ray.get(node_to_toggle.change_status.remote())
                    deaths -= 1

    thread = threading.Thread(target=loop)
    thread.start()


def close():
    global running, condition
    running = False
    with condition:
        condition.notify()
    # name_node_handle = get_or_create_name_node()
    # for node_handle in ray.get(name_node_handle.get_storage_nodes.remote()):
    #     ray.kill(node_handle)

    # ray.kill(name_node_handle)
    ray.shutdown()
