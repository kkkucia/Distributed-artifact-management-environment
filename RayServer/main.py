import storage_system

def main():
    storage_system.main()
    while True:
        user_input = input("Enter command (upload/update/delete/get/list/status/exit): ")

        if user_input == "upload":
            artifact_name = input("Enter artifact name: ")
            artifact_content = input("Enter artifact content: ")
            storage_system.upload_artifact(artifact_name, artifact_content)
            print(f"Artifact '{artifact_name}' uploaded.")

        elif user_input == "update":
            artifact_name = input("Enter artifact name: ")
            new_content = input("Enter new artifact content: ")
            storage_system.update_artifact(artifact_name, new_content)
            print(f"Artifact '{artifact_name}' updated.")

        elif user_input == "delete":
            artifact_name = input("Enter artifact name: ")
            storage_system.delete_artifact(artifact_name)

        elif user_input == "get":
            artifact_name = input("Enter artifact name: ")
            artifact_content = storage_system.get_artifact(artifact_name)
            print(f"Artifact '{artifact_name}': {artifact_content}")

        elif user_input == "list":
            second_input = input("What do you wana list? artifacts/nodes/node: ")
            if second_input == "artifacts":
                storage_system.list_artifacts()
            elif second_input == "nodes":
                print(storage_system.list_nodes())
            elif second_input == "node":
                node_id = int(input("Enter node id: "))
                print(storage_system.get_node_info(node_id))

        elif user_input == "status":
            storage_system.list_statuses()

        elif user_input == "exit":
            print("Storage server closed.")
            storage_system.close()
            break
        else:
            print("Invalid command. Please try again.\nAvailable commands: upload/update/delete/get/list/status/exit\n")


if __name__ == "__main__":
    main()
