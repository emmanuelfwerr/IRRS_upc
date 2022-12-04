from Edge import Edge

class Airport:
    def __init__ (self, iden=None, name=None):
        self.code: str = iden
        self.name: str = name
        self.incoming: dict[str, int] = dict()
        self.outgoing: dict[str, int] = dict()
        self.outweight = None
        self.current_page_rank = 0
        self.previous_page_rank = 0

    def __repr__(self):
        return f"{self.code}\t{self.current_page_rank}\t{self.name}"
    
    def update_outweight(self) -> None:
        self.outweight = sum(self.outgoing.values())