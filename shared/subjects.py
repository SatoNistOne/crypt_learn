class Subjects:
    WALLET_CLIENT = "wallet.fallback.client"
    WALLET_REPLICATE = "wallet.fallback.replicate"
    WALLET_RESYNC = "wallet.fallback.resync"
    RAFT_REQUEST_VOTE = "raft.requestvote"
    RAFT_HEARTBEAT = "raft.heartbeat"

    @staticmethod
    def order_place(pair: str) -> str:
        return f"orders.place.{pair}"

    @staticmethod
    def order_cancel(pair: str) -> str:
        return f"orders.cancel.{pair}"

    @staticmethod
    def trade_exec(pair: str) -> str:
        return f"trades.executed.{pair}"