class LoadClient:
    
    def __init__(self) -> None:
        self.active = True
        print("Client is initialized.")
    
    def load(self, value):
        if self.active:
            print(f"Loaded Value=[{value}]")
        else:
            raise Exception("Client is closed!")
    def close(self):
        self.active = False
        print("Client is closed.")

    
    
    