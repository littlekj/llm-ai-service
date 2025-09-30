class UserAlreadyExistsError(Exception):
    def __init__(self, field: str):
        self.message = f"{field} already registered"
        super().__init__(self.message)
        
        
class AuthenticationError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)